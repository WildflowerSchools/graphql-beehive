#!/bin/bash

version=$GIT_TAG
text="built by travis"
branch=$TRAVIS_COMMIT
repo_full_name="WildflowerSchools/graphql-beehive"
token=$GIT_TOKEN


generate_post_data()
{
  cat <<EOF
{
  "tag_name": "$version",
  "target_commitish": "$branch",
  "name": "$version",
  "body": "$text",
  "draft": false,
  "prerelease": false
}
EOF
}

echo "Create release $version for repo: $repo_full_name branch: $branch"
curl --data "$(generate_post_data)" "https://api.github.com/repos/$repo_full_name/releases?access_token=$token"
