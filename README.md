# Go Pipeline Client

This project is a client written in Go for Adobe Pipeline. This client library
allows you to both send messages to and receive messages from Adobe Pipeline.
Moreover, if you decide to manually manage your read offset, it exposes a method
to do so.

## Install

```
go get github.com/adobe/pipeline-go
```

## Authentication

This library doesn't implement any authentication. The way you authenticate to
Adobe Pipeline is abstracted away by the `pipeline.TokenGetter` interface. You
need to implement your own strategy to obtain an authorization token.

Adobe Pipeline uses IMS for authentication. Have a look at [this client
library](https://github.com/adobe/ims-go) for implementing an authorization flow
with IMS. Look at the godoc for an example of how to implement
`pipeline.TokenGetter` to authenticate via IMS.

## Sending messages

You can send messages to Adobe Pipeline by using the `Send()` method of the
`pipeline.Client`. Look at the godoc for relevant examples. 

## Receiving messages

You can receive a stream of messages by calling the `Receive()` method of the
`pipeline.Client`. The method returns a channel you can range over to read
messages from Adobe Pipeline. The channel will be closed when the
`context.Context` you pass to `Receive()` expires.

The library will automatically handle error conditions and reconnect to Adobe
Pipeline, so you can focus on consuming the message and leave connection issues
to the library. The library will still pass on the channel every error it
encounters while interacting with Adobe Pipeline, so you can log them or react
on them if you really want.

Look at the godoc for relevant examples.

## Manage the read offset

Adobe Pipeline allows you to manually manage the read offset by sending back a
sync marker that the API will send periodically to you. Look at the godoc for
relevant examples.

## Fine-tuning the HTTP connection

This library doesn't expose any low-level configuration option to control the
HTTP connection to Adobe Pipeline. Instead, it is your responsibility to
configure an `http.Client` with the appropriate timeouts for your use case. If
you are not sure about the options at your disposal, start by reading the
[official documentation](https://golang.org/pkg/net/http/#Client) for the
`http.Client`. If you have never experimented with these options, [this
guide](https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/)
digs deeper in the different timeouts used by the `http` and `net` packages in
Go.

## Contributing

Contributions are welcomed! Read the [Contributing
Guide](./.github/CONTRIBUTING.md) for more information.

## Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for
more information.