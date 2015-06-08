from pprint import print

import pygit2

# Create a repository (bare or not)
# pygit2.init_repository('/home/tony/tmp', True)

# or clone one
# pygit2.clone_repository(repo_url, repo_path)

repo_url = 'https://github.com/ardumont/dot-files.git'
parent_repo_path = '/home/tony/repo/perso/dot-files'

# discover the repo
repo_path = pygit2.discover_repository(parent_repo_path)

# Now we can build the repo's python representation
repo = pygit2.Repository(repo_path)

last = repo[repo.target.head]
# last.id
# last.message

# git log --all and display only the commit messages:
for commit in repo.walk(last.id, pygit2.GIT_SORT_TIME):
    print(commit.message)  # or some other operation

other_commit = repo.revparse_single('HEAD')

print isinstance(other_commit, pygit2.Object)  # object in the git sense

# >>> last.tree
# <_pygit2.Tree object at 0x7f4fef61d990>
# >>> len(last.tree)
# 76
# >>> for entry in last.tree:
# ...    print(entry.id, entry.name)
# ...
# af5e21235f11395f53168d8b8154d875cea1bb6b .FBReader
# 6b8ce12e883775c27985f95c002a5b81b690a022 .Xmodmap
# 8c72915ddbf498ebe4fe03f1157318340422a8d3 .Xresources
# baddfac6045f646885f6664d9c07614af1dcce9a .bashrc
# c0c0b02d03c1b3ad568dbceca92590e1af11d756 .conkerorrc
# 5182b65f4c250aaa12c2d246424e87fd51760568 .ctags
# 22fa37ca23570831f64e38f37d7b2a104c5dceaf .ghc
# 2a63644ee00b60378783069effc7fb3391aecc35 .gimpgimp-layout-1920x1080
# 13f229130eb4476aad30fd10001477ee607af72c .gimpgimp-rules
# 5a8309076dc633f431797c4bd59876490b0d7408 .git-completion.bash
# 5c69716f52d949c08f39cdb76e8294c759fac325 .gitconfig
# 2067caa145a94beda293f0bbb350b7bec5658844 .gitignore
# 65f173cbb57373ab214960f4545092b5b73b23bf .gitignore_global
# b7fd1b329312bec4934083d2ae6ab27323258aea .keysnail.js
# 96a9485c44502d21c5eaa10657da73265770c721 .layouts
# 7f43cd1a6f8ebb7b2eda543e2a17c6a81ac889c0 .mc-lists.el
# 5e2ea7701a43292685ab1d593bc4d023f3ad0cfa .nixpkgs
# be5354f7b2734ed70f0f53e1822a488e1b868584 .offlineimap-helpers.py
# 8cefef9244f3fe699722f5bdb8bb4c20e7d872b6 .offlineimaprc
# 89be4b5e1f41da0ff3d19c8459dd02c5897f6257 .profile
# dc62799285e3ab59d6f1de0f2d1e50dbacc8fe20 .ratpoisonrc
# fd62e2707486375e922d42000e6b8241310e0ad4 .sbclrc
# d0f5199e3ada998e427b693c8a4edaa8e2e2389d .sbt
# 433404fab7f9f65829103f4cca28abe58f2519c3 .shrc
# 86332abc416bc77d0d202d2518fcb0fce3c9f0ed .shrc-aliases
# caf08d296f7d2ea49588874671e66f76acc1dc2d .shrc-env
# a7dbe042d803d240f821f1b075ad4f0a4c979f62 .shrc-nix-env
# b300ef90ff3b14de5d22fe9564e01db7cbbd8336 .shrc-path
# 4a692049cec905dae56b9cab1a89449faf23d5ce .shrc-prompt
# 9e2eb70f67fcf18d5bbd965956cd74778508b19c .shrc-work
# bd33e3364ac5ca602e065b731ee44e3c365eb48c .signature
# 2ff054dfeaf29ecf6d6dcc489c21dc1294324b0a .signature2
# e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 .stalonetrayrc
# 1c5bfac427697f1a622412d3cfc3f5f73cd514e8 .stumpwm-functions.lisp
# efd1c2c9435d67aeac81448abaea8e131dad92ef .stumpwm.start
# 3f2550039a84b4ea2769d011db3d7f5ca5148f34 .stumpwmrc
# 95fb6c25cbbf453aabd9696b55f232c224811e5c .tmux.conf
# 2f34906d2f7c9e01514dda6252c2fa7488c5cd1f .travis.yml
# c6367a1e212377b30976c5bbf14859432fa79fa5 .urxvt
# 5eae3f781fd00d6a37759636539456fcf4a9b3eb .vimrc
# deb4c56b44dd41b3a193e618fa253fd8683c6650 .xinit
# ece7a843d0ec308be16df4cf29a33ad467617c21 .xinitrc
# 3616e7e563b9288320c839de3a72ce83692fe625 .xmobarrc
# e1140398e58a105e557eb41f8d72aa5b5fe61ca7 .xmonad.start
# 8b149bb16743439ef4acd5ff3236bab1092fdeb7 .xmonad
# 717edbae719d27a55b12d57d8acdbb467643e2a4 .xscreensaver
# 03280dfd868a0cfa0acc868715f44960e007ba6b .xsession
# 147611c748ad40639c7029bcd6ca6f814ba40059 .zshenv
# aad5bc6b6a4ceed342ce0885ea9c2ba5dfef2477 .zshrc
# 1a066eb2c758b8583dd61d50fbe776af36069222 Makefile
# fa181e0a0fb895dfd656ba1859034ae70921bf88 README.md
# c73c7d525f18a26812f86027fe14c4a4080874d3 adjust-system-configuration.sh
# 6f0b6fa22cc55bd0c809f417eab0a4abd7fecf8c deploy-emacs-live-packs.sh
# 81f54bafa332b55bef6a686ae17c4b995d9bb697 deploy-emacs-prelude-packs.sh
# 38894b3a5c3c4365a47e91afb47d63b0f187af69 deploy-emacs.sh
# ad6f0c21c5cf91e61c955ee603bd456477243b90 deploy-keysnail.sh
# 7a3574976359d51976865cf8a5f1a2ac27f78a3b deploy-platform.sh
# 250b984d9a4e5606cea4005d52453a02999a4e1a deploy.sh
# 8534792bd9b42579531fdb3ff922ba301423f2a4 gpg-agent.conf
# b46c4d41a39917b2d159572747c766ee685ce707 gpg.conf
# 19095cabaebbc34ae837d2b4c23b1aeea2fc5837 gradle.properties
# e5f2e170732d44c1ff2b80ceb6c1075b34f95d41 hs
# 078273c1a96a613839e63e5833e45205001b354b init.gradle
# b1aceb92073cf6858d9fb176f29b1ac3866dff6b keysnail
# 5c99e16b92703e5fed17cfc9e3cba57611538cc0 keysnail@mooz.github.com
# 5f036b439b40e30946af1d93308466de00884cba lighttable-settings
# ad6a9226bc000a8a348a01f7ac03e880bbe02ae0 nixos
# 2211c7b4b1ec019c1ae1ff7ae82c21288e91aad3 profiles.clj
# 03778a552d4027505e3deba1f119f0d7170be5ae project
# 8372e87e2dd7d15b7a9209f6b45d82bb657f1341 quicklisp.lisp
# af439d5b7f0f6a2d7cc1ba328ccbc8edd29d6c45 quicklisp
# 84645ea867954625224e152eae57a3c3fe0cbb8f run-travis-ci.sh
# e9c3fbd0aff0cfca6ac9f6e90b855e6a07dda7b1 settings-idea12.jar
# 6dff35ce2fe2b4d3b5cef5f52c3ab71b63e9f443 settings-idea13.jar
# 1565d9c2e35f040dd92f11e48b98ecd14cdf859b settings-idea14.jar
# 0b3511a422fb77bbd13c61e20a3348575ad11b7a stumpwm-macros.lisp
# >>> entry_by_name = last.tree['profiles.clj']
# >>> entry_by_name
# <_pygit2.TreeEntry object at 0x7f4ff35f7678>
# >>> entry_by_name.id
# 2211c7b4b1ec019c1ae1ff7ae82c21288e91aad3
# >>> entry_by_name.name
# 'profiles.clj'
# >>> entry_by_name.hex
# '2211c7b4b1ec019c1ae1ff7ae82c21288e91aad3'
# >>> repo[entry_by_name.id]
# <_pygit2.Blob object at 0x7f4fef61d8d0>
# >>> object_entry_by_name=repo[entry_by_name.id]
# >>> isinstance(object_entry_by_name, pygit2.Blob)
# True

import psycopg2

conn = psycopg2.connect("dbname=swhgitloader user=tony")
cur = conn.cursor()

cur.execute("select sha1 from object_cache where type = 0")

cur.execute("select sha1 from object_cache where type = 0")
cur.fetchone()
#>>> ('4724caa4676e992d2705d498506ee34bd80c0e6d',)

