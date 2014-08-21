## flume-spooling-directory-sink

An [Apache Flume](https://flume.apache.org/) sink that spools to a directory and periodically rotates the temporary files into another directory.

## Example config

```
...
agent.sinks.directoryFileSink.type = com.unrulymedia.flume.SpoolingDirectoryFileSink
agent.sinks.directoryFileSink.channel = someChannel
agent.sinks.directoryFileSink.sink.directory = target/
agent.sinks.directoryFileSink.sink.rollInterval = 5
...
```

## Motivation

The built-in flume [SpoolingDirectorySource](https://flume.apache.org/FlumeUserGuide.html#spooling-directory-source) does not have an inverse sink (as the FileSink does not work in this way) so the SpoolingDirectoryFileSink is an implementation of this.

This enables us to easily create Flume topologies with spooling reliability in-between for resiliency.

## Installation

Create a jar with `mvn package` and include in the flume [plugins.d](https://flume.apache.org/FlumeUserGuide.html#installing-third-party-plugins) directory. 

## License

MIT