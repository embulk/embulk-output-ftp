## 0.2.2 - 2019-07-16
* [maintenance] Fix unclosed resource to prevent temp file deletion failure on Windows [#24](https://github.com/embulk/embulk-output-ftp/pull/24)

## 0.2.1 - 2018-12-14
* [maintenance] Update Embulk version v0.8.9 to v0.9.11 and refactor code for Java8 support [#21](https://github.com/embulk/embulk-output-ftp/pull/21)
## 0.2.0 - 2018-07-04
* [maintenance] Only support Java8 [#16](https://github.com/embulk/embulk-output-ftp/pull/16)
* [maintenance] Use embulk-util-ftp from Bintray [#15](https://github.com/embulk/embulk-output-ftp/pull/15)

## 0.1.7 - 2017-02-24

* [maintenance] Don't retry when Code:550(Permission denied) error happens [#13](https://github.com/embulk/embulk-output-ftp/pull/13)

## 0.1.6 - 2016-09-09

* [maintenance] Fix NullPointerException at FtpFileOutput.getRemoteDirectory() method [#12](https://github.com/embulk/embulk-output-ftp/pull/12)


## 0.1.5 - 2016-08-08

* [maintenance] Improve connection stability [#9](https://github.com/embulk/embulk-output-ftp/pull/9)

## 0.1.4 - 2016-07-29

* [maintenance] Fix default port number generation logic [#7](https://github.com/embulk/embulk-output-ftp/pull/7)

## 0.1.3 - 2016-07-21

* [new feature] Support both of FTPS(explicit) and FTPS(implicit). [#4](https://github.com/sakama/embulk-output-ftp/pull/4)
* [maintenance] Fix exception handling. [#5](https://github.com/sakama/embulk-output-ftp/pull/5)

## 0.1.2 - 2016-07-21

* [maintenance] Force to create remote directory if remote directory doesn't exists. [#2](https://github.com/sakama/embulk-output-ftp/pull/2)
* [maintenance] Add additional environmental variables for unit test. [#3](https://github.com/sakama/embulk-output-ftp/pull/3)

## 0.1.1 - 2016-07-20

* [maintenance] Add unit test, refactoring, change logging logic

## 0.1.0 - 2016-07-20

* [new feature] First Release
