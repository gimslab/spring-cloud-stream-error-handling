package com.gimslab.springcloudstreamtest2

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.BinderHeaders
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.SubscribableChannel
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.messaging.support.MessageBuilder
import java.util.concurrent.atomic.AtomicInteger

@SpringBootApplication
@EnableBinding(TwoOutputProcessor::class)
class SpringCloudStreamExam2Application(
		val twoOutputProcessor: TwoOutputProcessor
) {

	@StreamListener("binding1")
//	@SendTo("error")
	fun normProc(msg: Message<*>) {
		val X_RETRIES_HEADER = "x-retries"
		println("MMMMMMMMMMMM" + msg)
		if (msg != null) {
			val retries = getRetries(msg, X_RETRIES_HEADER)
			println("Error On " + retries)
//			return MessageBuilder.fromMessage(msg)
//					.setHeader(BinderHeaders.PARTITION_OVERRIDE,
//							msg.headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
//					.build()
			twoOutputProcessor.errorOut().send(
					MessageBuilder.fromMessage(msg)
							.setHeader(BinderHeaders.PARTITION_OVERRIDE,
									msg.headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
							.build())
		}
	}

	@StreamListener("input")
	@SendTo("output")
	fun process(failed: Message<*>): Message<*>? {
		val X_RETRIES_HEADER = "x-retries"
		val retries = getRetries(failed, X_RETRIES_HEADER)
		println("error.so8400out.so8400 On retries:" + retries)
		if (retries == null) {
			println("First retry for " + failed)
			return MessageBuilder.fromMessage(failed)
					.setHeader(X_RETRIES_HEADER, 1 as Int)
					.setHeader(BinderHeaders.PARTITION_OVERRIDE,
							failed.headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
					.build()
		} else if (retries < 2) {
			println("Another re(try for " + failed)
			return MessageBuilder.fromMessage(failed)
					.setHeader(X_RETRIES_HEADER, retries + 1)
					.setHeader(BinderHeaders.PARTITION_OVERRIDE,
							failed.headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
					.build()
		} else {
			println("Retries exhausted for " + failed + " two=" + twoOutputProcessor)
			twoOutputProcessor.parkingLot().send(MessageBuilder.fromMessage(failed)
					.setHeader(BinderHeaders.PARTITION_OVERRIDE,
							failed.headers.get(KafkaHeaders.RECEIVED_PARTITION_ID))
					.build())
		}
		return null
	}

	private fun getRetries(msg: Message<*>, X_RETRIES_HEADER: String) =
			msg.headers.get(X_RETRIES_HEADER)?.toString()?.toInt()
}

interface TwoOutputProcessor : Processor {
	@Output("parkingLot")
	fun parkingLot(): MessageChannel

	@Input("binding1")
	fun binding1(): SubscribableChannel

	@Output("binding1")
	fun binding1Error(): MessageChannel

	@Output("error")
	fun errorOut(): MessageChannel
}

fun main(args: Array<String>) {
	runApplication<SpringCloudStreamExam2Application>(*args)
}

