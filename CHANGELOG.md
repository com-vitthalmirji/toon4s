# Changelog

## [0.2.1] - 2025-11-10


### Bug fixes
- set GITHUB_REF env var for sbt-ci-release tag detection ([`e762bc4`](https://github.com/vim89/toon4s/commit/e762bc40e17b90d4b5a4fa8b105078e0f423b5c0)) by @vim89



### Documentation
- update CHANGELOG.md for main [skip ci] ([`574ae62`](https://github.com/vim89/toon4s/commit/574ae623fc4363d33eb9a1dd08ae7329483b633c)) by @github-actions[bot]


## [0.2.0] - 2025-11-09


### Bug fixes
- auto-tag now triggers release and changelog workflows ([`e3812f3`](https://github.com/vim89/toon4s/commit/e3812f306c5eab19c311504437be64107d789fbb)) by @vim89
- add actions write permission to auto-tag workflow ([`303ee24`](https://github.com/vim89/toon4s/commit/303ee246eefacf7bf97d8fcc74f7931c3b864040)) by @vim89
- use repository_dispatch to trigger release workflows ([`ac28ac3`](https://github.com/vim89/toon4s/commit/ac28ac36287d815c2bc28a7de4b452ee571912a2)) by @vim89
- changelog triggers on repository_dispatch ([`6e8bb12`](https://github.com/vim89/toon4s/commit/6e8bb1253c5a0086bb52543db0af2a007df9e082)) by @vim89
- use env vars in github-script to avoid syntax errors ([`06f9e32`](https://github.com/vim89/toon4s/commit/06f9e32c51fd1a62c391bfb7e46228c8dcbbc12c)) by @vim89
- add pull-requests write permission for ci workflow ([`a8a5549`](https://github.com/vim89/toon4s/commit/a8a5549c56a946d04a9db681b63997a680192f98)) by @vim89
- changelog triggers on repository_dispatch ([`78a3af2`](https://github.com/vim89/toon4s/commit/78a3af20c29df1e9db3fd4b8b2fe6e7cf0534826)) [#17](https://github.com/vim89/toon4s/pull/17) by @vim89
- checkout exact tag ref for sbt-ci-release to detect release version ([`572b1da`](https://github.com/vim89/toon4s/commit/572b1da28a92dc3a1d026dc2f7fd7cc475c664d8)) by @vim89
- add required permissions for workflow triggers and PR sync ([`d9a56b6`](https://github.com/vim89/toon4s/commit/d9a56b69d72712d72aa2846a217c3a84e6eb8ba7)) by @vim89



### Documentation
- update CHANGELOG.md for main [skip ci] ([`3366b9a`](https://github.com/vim89/toon4s/commit/3366b9a3250c685871d1be04b75db6b89fb4db7d)) by @github-actions[bot]
- update CHANGELOG.md for main [skip ci] ([`d72a656`](https://github.com/vim89/toon4s/commit/d72a656130d998bde7eb18af5dcaccd85c2d4c30)) by @github-actions[bot]
- update CHANGELOG.md for main [skip ci] ([`1f8d148`](https://github.com/vim89/toon4s/commit/1f8d148c091c90ae4a180e86b17e1ae855330ac6)) by @github-actions[bot]
- test ([`a6578eb`](https://github.com/vim89/toon4s/commit/a6578eb171f149a863c004da5f21f4242bc48139)) by @vim89



### Features
- add commit hashes and PR links to changelog ([`ca99ed2`](https://github.com/vim89/toon4s/commit/ca99ed2b0d4a74ee554aa7ffc61adfce512714ac)) by @vim89
- enhance changelog to support forked PRs and show contributors ([`ca79b56`](https://github.com/vim89/toon4s/commit/ca79b5660b49e7ab96c4bb5141f42bf30eac45ad)) by @vim89
- changelog updates after release with auto PR branch sync ([`4f2d83b`](https://github.com/vim89/toon4s/commit/4f2d83baac2ae7290529f5ad91cf6de9462e8d0d)) by @vim89
- changelog updates after release with auto PR branch sync ([`d1c4ff2`](https://github.com/vim89/toon4s/commit/d1c4ff2243aa7145b21fb06cd21550ea79d20a3e)) [#21](https://github.com/vim89/toon4s/pull/21) by @vim89



### Chroe
- updated release notes ([`2b357ba`](https://github.com/vim89/toon4s/commit/2b357bab9291df81ff50c79c9ed8feb52caf9df0)) by @vim89


## [0.1.2] - 2025-11-09


### Bug fixes
- update github workflows and dependencies ([`6a5c667`](https://github.com/vim89/toon4s/commit/6a5c66718d3b9d75bb27ddac707336a3180212d1)) by @vim89
- auto-tag paths-ignore and ci duplication issues ([`a307025`](https://github.com/vim89/toon4s/commit/a307025340c9689372319a5f26f676c67b1f1158)) by @vim89


## [0.1.1] - 2025-11-09


### Bug fixes
- Simplify release.yml ([`e7d2c5c`](https://github.com/vim89/toon4s/commit/e7d2c5c558e5379aa4216bb8946b6935db5c7f9a)) by @vim89



### Chores
- run scalafmt ([`5c37b95`](https://github.com/vim89/toon4s/commit/5c37b953b39d4739f6c4676c8aa2c9f90859aae0)) by @He-Pin
- Add toonResult ([`9ab7e09`](https://github.com/vim89/toon4s/commit/9ab7e096a3c99f3f2379dfb2adf3773218e59bfb)) by @He-Pin
- Rewrite ToonResult in Scala ([`02fadc9`](https://github.com/vim89/toon4s/commit/02fadc925c35941c5273026cbe92dd73c45da10b)) by @He-Pin



### Documentation
- [skip ci] [skip release] ([`0653a0b`](https://github.com/vim89/toon4s/commit/0653a0b15f399b548da7d309e8985012c970b728)) by @vim89
- update CHANGELOG.md [skip ci] ([`2ab0e03`](https://github.com/vim89/toon4s/commit/2ab0e03be370d5368c13cf94a567f69eb53599c5)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`667f00f`](https://github.com/vim89/toon4s/commit/667f00f9f8917b23462a5eff10be31a5f939df5f)) by @github-actions[bot]
- Update README to reflect JVM support instead of Scala ([`3f66504`](https://github.com/vim89/toon4s/commit/3f665048c7ffb19a573977fa14daa5ea67bd0b6e)) by @vim89
- update CHANGELOG.md [skip ci] ([`b2ad356`](https://github.com/vim89/toon4s/commit/b2ad356089a43d3a0ca0a92fb11a638becf51763)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`667ec86`](https://github.com/vim89/toon4s/commit/667ec86851bedc4835eec8cef1c348608626acdc)) by @github-actions[bot]
- Updated README.md and SCALA-TOON-SPECIFICATION.md ([`5b50d69`](https://github.com/vim89/toon4s/commit/5b50d690a970e628e90cff771a271c88c4eddc01)) by @vim89
- update CHANGELOG.md [skip ci] ([`d4afaab`](https://github.com/vim89/toon4s/commit/d4afaab2a5996390f1d8d37118369cf50b2d3120)) by @github-actions[bot]



### Features
- add workflow_dispatch trigger for manual releases ([`f7e8ad5`](https://github.com/vim89/toon4s/commit/f7e8ad5fc8c737625e17f445f511d9f615a44a73)) by @vim89



### Chroe
- updated benchmarks to log in CI for forked repo PR and comment on PRs ([`54a33b5`](https://github.com/vim89/toon4s/commit/54a33b5db1f9f7145b10ca2891bc67d2c73def64)) by @vim89
- Scaladocs bug-fixes ([`e02ce50`](https://github.com/vim89/toon4s/commit/e02ce508630b852d232404eb6ce1bf5cba169880)) by @vim89
- Fix scaladocs issue ([`bf92e69`](https://github.com/vim89/toon4s/commit/bf92e698ccb7a0789496847cdb71356ed5e60d72)) by @vim89


## [0.1.0] - 2025-11-08


### Docs
- add deep links, quoting table + diagram; CLI: --stats flag ([`3a3abb6`](https://github.com/vim89/toon4s/commit/3a3abb6b1aa49000f1dee6f199901afafaa47913)) by @vim89



### Documentation
- update CHANGELOG.md [skip ci] ([`77b486c`](https://github.com/vim89/toon4s/commit/77b486c7cb8daaafab3f7337f66cdb29ba2a8388)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`078af0e`](https://github.com/vim89/toon4s/commit/078af0e41389eef744de3cbaf189b28aeedf90b7)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`1b59f7d`](https://github.com/vim89/toon4s/commit/1b59f7d6cbf33d1da446987513eb0ee806d75d6d)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`e6d4e62`](https://github.com/vim89/toon4s/commit/e6d4e62a1aa17411b67bd7552a4a80d4f43b43c8)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`50d94d8`](https://github.com/vim89/toon4s/commit/50d94d82fb497319520a6891b5c6132d13d6b737)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`1e75de6`](https://github.com/vim89/toon4s/commit/1e75de6686bb85943907acde81529499a8092ec1)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`56d674d`](https://github.com/vim89/toon4s/commit/56d674d9163630f304c142a4a218237eba63d459)) by @github-actions[bot]
- update CHANGELOG.md [skip ci] ([`644342d`](https://github.com/vim89/toon4s/commit/644342d24c3f340b5b97b20be0b0ec6a2586d7f9)) by @github-actions[bot]



### Features
- feat: initial TOON format implementation. ([`39d0973`](https://github.com/vim89/toon4s/commit/39d0973d6b0b68c22f724b432a84414466c0eee5)) by @vim89
- feat: initial TOON format implementation. ([`30e7e05`](https://github.com/vim89/toon4s/commit/30e7e0536992a453c9f599018d35dbd1a94d9d8a)) by @vim89
- feat: initial TOON format implementation. ([`3f7a1a9`](https://github.com/vim89/toon4s/commit/3f7a1a9c8316ed3a6612fc669b8f746727052e3e)) by @vim89
- feat: initial TOON format implementation. ([`bd896f4`](https://github.com/vim89/toon4s/commit/bd896f4d67d4e20ff5a760518c0ad7c46fd1b3b1)) by @vim89
- feat: initial TOON format implementation. ([`efbf8ae`](https://github.com/vim89/toon4s/commit/efbf8ae7913030b2a5cb92ae6ce0e8d710e186db)) by @vim89
- feat: initial TOON format implementation. ([`f128e46`](https://github.com/vim89/toon4s/commit/f128e4645c17ed0aaca306250801cd44b7347295)) by @vim89
- feat: add line/column info to decode errors ([`ebb8569`](https://github.com/vim89/toon4s/commit/ebb856989c235316a1bf517bc4f6dd6366d4db68)) by @vim89
- feat: add line/column info to decode errors ([`164367a`](https://github.com/vim89/toon4s/commit/164367a53b584ac28ff8080d146262583bdbe550)) by @vim89
- feat: add line/column info to decode errors ([`75bcf2c`](https://github.com/vim89/toon4s/commit/75bcf2c4ac5bdeac5c2505ccf70912e534180ba4)) by @vim89
- feat: add line/column info to decode errors ([`b673683`](https://github.com/vim89/toon4s/commit/b673683dafb5d73701be41f86213356ac0d88158)) by @vim89
- Built from the ground up with idiomatic Scala, toon4s delivers: ([`6ab3f93`](https://github.com/vim89/toon4s/commit/6ab3f939a285335e49d08028fc3a18b970bf19b0)) by @vim89
- Built from the ground up with idiomatic Scala, toon4s delivers: ([`5d82210`](https://github.com/vim89/toon4s/commit/5d822102b7fc0140fafb341bf0538e97e89d6199)) by @vim89
- Built from the ground up with idiomatic Scala, toon4s delivers: ([`916a7c7`](https://github.com/vim89/toon4s/commit/916a7c76c94210ef61091831eee6f837773a49cd)) by @vim89
- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture ([`c3286f8`](https://github.com/vim89/toon4s/commit/c3286f8e2328352cfe51dd7bddf3d6b270f140d5)) by @vim89
- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture ([`cd62a96`](https://github.com/vim89/toon4s/commit/cd62a96c955f28cabf3352d547bb7cc27ff88900)) by @vim89
- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture ([`a3d1c89`](https://github.com/vim89/toon4s/commit/a3d1c8964a056cc7cd58ff3af3af2c8bf29dd884)) by @vim89
- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture ([`83ad3f1`](https://github.com/vim89/toon4s/commit/83ad3f1ec1672ed3216376935ffbca8465818623)) by @vim89
- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture ([`13cc026`](https://github.com/vim89/toon4s/commit/13cc0265f41ba3e64c6631d6497098212ed46931)) by @vim89
- toon4s: Token-Oriented Object Notation for Scala ([`c13ee97`](https://github.com/vim89/toon4s/commit/c13ee973dbe1ee39729ea97a060f576b27d85fa7)) by @vim89
- toon4s: Token-Oriented Object Notation for Scala ([`a044adb`](https://github.com/vim89/toon4s/commit/a044adb6c6e837dd1639d146b4ee7852e4430f16)) by @vim89
- toon4s: Token-Oriented Object Notation for Scala ([`8e5fde1`](https://github.com/vim89/toon4s/commit/8e5fde1a21634e61a4331e1caaeabbdf852aa9b9)) by @vim89
- toon4s: Token-Oriented Object Notation for Scala ([`2f55602`](https://github.com/vim89/toon4s/commit/2f55602a170b4429ad696edf7c9f950d7a51bc82)) by @vim89


