package com.demo.aws.docker.config

case class DemoConfig(appName: String, source: Input, dest: Output)

case class Input(input: Source)

case class Output(output: Source)

case class Source(path: String)
