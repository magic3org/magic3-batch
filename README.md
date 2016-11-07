Magic3 batch process program
(Magic3バッチ処理プログラム)
====

## Description(説明)

Batch process program do long term process which web process normaly can't do.
This is launched by cron.
The main features is as below.

1.Sum up accesslog for web analitics

バッチ処理プログラムは通常Webから処理できない処理時間の長い処理を行います。
クーロンで起動します。主な処理は以下の通りです。

1.アクセス集計処理

Development Language(開発言語): Go

## Usage(使い方)
Launch by shell.
```bash
$ magic3-batch -path=[Magic3 siteDef.php file path]

Example
```bash
$ magic3-batch -path=/var/www/html/magic3/include/siteDef.php

