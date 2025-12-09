#!/bin/bash
# Helper script to merge PR with co-author attribution
#
# Usage:
#   ./merge-with-coauthor.sh PR_NUMBER "Author Name" "author@email.com"
#
# Example:
#   ./merge-with-coauthor.sh 42 "Rory Graves" "rory.graves@thetradedesk.com"

set -e

if [ $# -ne 3 ]; then
    echo "Usage: $0 PR_NUMBER \"Author Name\" \"author@email.com\""
    echo "Example: $0 42 \"Rory Graves\" \"rory.graves@thetradedesk.com\""
    exit 1
fi

PR_NUMBER=$1
AUTHOR_NAME=$2
AUTHOR_EMAIL=$3

echo "Fetching PR #${PR_NUMBER} details..."

# Get PR title and body
PR_TITLE=$(gh pr view $PR_NUMBER --json title --jq '.title')
PR_BODY=$(gh pr view $PR_NUMBER --json body --jq '.body')

# Create merge commit message with co-author
COMMIT_MESSAGE=$(cat <<EOF
${PR_TITLE}

${PR_BODY}

Co-authored-by: ${AUTHOR_NAME} <${AUTHOR_EMAIL}>
EOF
)

echo ""
echo "Will merge PR #${PR_NUMBER} with the following message:"
echo "─────────────────────────────────────────"
echo "$COMMIT_MESSAGE"
echo "─────────────────────────────────────────"
echo ""

read -p "Proceed with merge? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    gh pr merge $PR_NUMBER --merge --body "$COMMIT_MESSAGE"
    echo "✅ Successfully merged PR #${PR_NUMBER} with co-author attribution"
else
    echo "❌ Merge cancelled"
    exit 1
fi
