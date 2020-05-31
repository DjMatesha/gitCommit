import pandas as pd
import numpy as np
import github
import progressbar
import schedule
import os.path
import time


# explode two or more column in dataframe
def _explode(df, lst_cols, fill_value=''):
    idx_cols = df.columns.difference(lst_cols)
    lens = df[lst_cols[0]].str.len()
    idx = np.repeat(df.index.values, lens)
    res = (pd.DataFrame({col: np.repeat(df[col].values, lens) for col in idx_cols}, index=idx)
           .assign(**{col: np.concatenate(df.loc[lens > 0, col].values) for col in lst_cols}))
    if (lens == 0).any():
        res = (res.append(df.loc[lens == 0, idx_cols], sort=False).fillna(fill_value))
    res = res.sort_index()
    res.reset_index(drop=True, inplace=True)
    return res


# get index of last loaded data
def _get_next_number(name):
    try:
        temp_df = pd.read_csv(name, header=0)
        return temp_df.iloc[-1]['number'] + 1
    except Exception:
        return 0


# get repository
def get_repo(git_id, key, repo):
    gh = github.Github(git_id, key)
    repo = gh.get_repo(repo)
    return repo


# load and write commits data
def get_commits(repo, explode_file=True, filename=''):
    commits = repo.get_commits()
    commit_count = commits.totalCount
    bar = progressbar.ProgressBar(maxval=commit_count).start()
    if filename == '':
        filename = repo.name + '_commits.csv'

    def _load_commits():
        start = int(_get_next_number(filename))
        try:
            data = []
            for num, commit in enumerate(commits[start:]):
                files = []
                for i in commit.files:
                    files.append(i.filename)
                files_sha = []
                for i in commit.files:
                    files_sha.append(i.sha)
                data.append([num + start, commit.sha, "" if (commit.author is None) else commit.author.id,
                             "" if (commit.author is None) else commit.author.name, commit.commit.author.date, files,
                             files_sha])
                bar.update(num + start)
        except github.GithubException:
            _write_commits(data)
            print("\nAPI rate limit exceeded, please wait")
        except IndexError:
            # All data uploaded
            return schedule.CancelJob
        else:
            # All data downloaded
            _write_commits(data)
            return schedule.CancelJob

    def _write_commits(data):
        df = pd.DataFrame(data, columns=['number', 'sha', 'author_id', 'author_name', 'date', 'files', 'files_sha'])
        if explode_file:
            df = _explode(df, ['files', 'files_sha'])
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False,
                      columns=['number', 'sha', 'author_id', 'author_name', 'date', 'files', 'files_sha'])
        else:
            df.to_csv(filename, index=False,
                      columns=['number', 'sha', 'author_id', 'author_name', 'date', 'files', 'files_sha'])

    res = _load_commits()
    schedule.every().hour.do(_load_commits)
    if res != schedule.CancelJob:
        while True:
            schedule.run_pending()
            time.sleep(1)
    bar.finish()


def get_issues(repo, explode_time=True, explode_tags=True, explode_assignee=True, filename=''):
    if filename == '':
        filename = repo.name + '_issues.csv'

    def _load_issues_by_state(state):

        def _load_issues(issues):
            start = int(_get_next_number(filename))
            try:
                data = []
                for num, issue in enumerate(issues[start:]):
                    label = []
                    for i in issue.labels:
                        label.append(i.name)
                    assign = []
                    for i in issue.assignees:
                        assign.append(i.id)
                    data.append(
                        [num + start, issue.number, issue.state, assign, issue.user.id, label, issue.created_at,
                         issue.updated_at, issue.closed_at])
                    bar.update(num + start)
            except github.GithubException:
                _write_issues(data)
                print("\nAPI rate limit exceeded, please wait")
            except IndexError:
                # All data uploaded
                return schedule.CancelJob
            else:
                # All data downloaded
                _write_issues(data)
                return schedule.CancelJob

        def _write_issues(data):
            df = pd.DataFrame(data,
                              columns=['number', 'id', 'state', 'assignee', 'user', 'tags', 'created_at',
                                       'updated_at', 'closed_at'])
            if explode_tags:
                df = df.explode('tags')
            if explode_assignee:
                df = df.explode('assignee')
            if explode_time:
                df['activity'] = [['open_issue', 'change_issue', 'close_issue']] * df.shape[0]
                df['time'] = df.apply(
                    lambda row: [row['created_at'], row['updated_at'], row['closed_at']], axis=1)
                df = _explode(df, ['time', 'activity'])

            if os.path.exists(filename):
                if explode_time:
                    df.to_csv(filename, mode='a', header=False, index=False,
                              columns=['number', 'id', 'state', 'assignee', 'user', 'tags', 'time', 'activity'])
                else:
                    df.to_csv(filename, mode='a', header=False, index=False,
                              columns=['number', 'id', 'state', 'assignee', 'user', 'tags', 'created_at', 'updated_at',
                                       'closed_at'])
            else:
                if explode_time:
                    df.to_csv(filename, index=False,
                              columns=['number', 'id', 'state', 'assignee', 'user', 'tags', 'time', 'activity'])
                else:
                    df.to_csv(filename, index=False,
                              columns=['number', 'id', 'state', 'assignee', 'user', 'tags', 'created_at', 'updated_at',
                                       'closed_at'])

        closed_issues = repo.get_issues(state=state)
        closed_issues_count = closed_issues.totalCount
        bar = progressbar.ProgressBar(maxval=closed_issues_count).start()
        schedule.every().hour.do(_load_issues, issues=closed_issues)
        if _load_issues(issues=closed_issues) != schedule.CancelJob:
            while True:
                schedule.run_pending()
                time.sleep(1)
        bar.finish()

    print('\nDownloading closed issues')
    _load_issues_by_state('closed')
    print('\nDownloading open issues')
    _load_issues_by_state('open')
