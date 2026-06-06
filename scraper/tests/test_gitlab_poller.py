from __future__ import annotations

from app.gitlab_poller import GitCommit, _filter_new_commits, parse_commits

RAW_COMMITS = [
    # API returns newest-first
    {
        "id": "abcdef1234567890abcdef1234567890abcdef12",
        "author_name": "Alice",
        "title": "fix race in scheduler",
        "committed_date": "2026-06-01T12:00:00.000Z",
    },
    {
        "id": "0000000000000000000000000000000000000001",
        "author_name": "Bob",
        "title": "initial commit",
        "committed_date": "2026-05-30T10:00:00.000Z",
    },
]


def test_parse_commits_basic():
    commits = parse_commits(RAW_COMMITS)
    assert len(commits) == 2
    # sorted oldest-first
    assert commits[0].author == "Bob"
    assert commits[1].author == "Alice"
    assert len(commits[0].short_sha) == 8
    assert commits[0].committed_at.tzinfo is not None


def test_parse_commits_message_truncation():
    long_title = "x" * 600
    raw = [
        {
            "id": "abcdef1234567890abcdef1234567890abcdef12",
            "author_name": "Alice",
            "title": long_title,
            "committed_date": "2026-06-01T12:00:00.000Z",
        }
    ]
    commits = parse_commits(raw)
    # truncation happens in write_git_commits, not parse_commits — message stored as-is
    # but write_git_commits slices to 500; verify the raw message comes through full
    assert len(commits[0].message) == 600


def test_filter_new_commits_no_last_sha():
    commits = parse_commits(RAW_COMMITS)
    result = _filter_new_commits(commits, None)
    assert result == commits


def test_filter_new_commits_deduplication():
    commits = parse_commits(RAW_COMMITS)
    # commits[0] is Bob (older), commits[1] is Alice (newer)
    last_sha = commits[0].sha
    result = _filter_new_commits(commits, last_sha)
    assert len(result) == 1
    assert result[0].author == "Alice"
