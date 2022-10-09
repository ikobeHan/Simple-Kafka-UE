#pragma once

#include "Kismet/BlueprintFunctionLibrary.h"
#include "SimpleKafkaFunctionLib.generated.h"


DECLARE_DYNAMIC_DELEGATE_TwoParams(FKafkaConsumerCallback_BP, FString, TopicName, FString, Msg);

UCLASS()
class SIMPLEKAFKA_API USimpleKafkaFunctionLib : public UBlueprintFunctionLibrary
{
	GENERATED_BODY()

public:
	/*
	* 单例消费者,单个topic
	*/
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static bool StartConsumer(const FString& URL, const FString& TopicStr, const FString& GroudID = "0");
	/*
	* 单例消费者,多个topic
	*/
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static bool StartConsumerWithTopics(const FString& URL,TArray<FString> TopicsStr, const FString& GroudID = "0");

	//暂停或回复线程
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void SetPauseConsumer(bool Pause);

	//游戏进程销毁之前调一下，销毁下
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void StopConsumer();

	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void RegisterConsumerCallback(FKafkaConsumerCallback_BP Callback);


	/*
	* 生产者也单例
	*/
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static bool StartProducer(const FString& URL, const FString& TopicStr, int32 Partion = 0);

	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static bool SendMessag(const FString& Msg, const FString& KeyStr = "");

	//游戏进程销毁之前调一下，销毁下
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static void StopProducer();




	////////////////////////   通常方式 非单例   /////////////////////////////
	/*
	* 每个consumer一个topic的方式，需要groupID 不相同，否则只能一个consoumer接受到消息,需要自己管理句柄
	*/
	static class SimpleKafkaConsumer* CreateConsumerWithTopic(const FString& URL, const FString& TopicStr, const FString& GroupID = "0");
	static void RegisterConsumerCallback(class SimpleKafkaConsumer* Consumer,FKafkaConsumerCallback_BP Callback);
	static void StopConsumer(class SimpleKafkaConsumer* Consumer);

public:
	// ONLY for debug
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void TestConsumer();

	// ONLY for debug
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static void TestProduer();

private:
	static class SimpleKafkaConsumer* KafkaConsumerClient_;

	static class SimpleKafkaProducer* KafkaProducer;
};
