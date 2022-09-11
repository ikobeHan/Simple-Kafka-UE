// Copyright 1998-2019 Epic Games, Inc. All Rights Reserved.

#include "SimpleKafka.h"
#include "Projects/Public/Interfaces/IPluginManager.h"

#define LOCTEXT_NAMESPACE "FSimpleKafkaModule"

void FSimpleKafkaModule::StartupModule()
{
#if PLATFORM_64BITS
	//FString path = IPluginManager::Get().FindPlugin("SimpleKafka")->GetBaseDir();
	//FString dllpath = path + "/ThirdParty/RdKafka/x64_v142/bin/";
	//FPlatformProcess::AddDllDirectory(*dllpath);
	//FString dll1 = path + "/ThirdParty/RdKafka/x64_v142/bin/librdkafka.dll";
	//FString dll2 = path + "/ThirdParty/RdKafka/x64_v142/bin/librdkafkacpp.dll";
	//Dll1 = FPlatformProcess::GetDllHandle(*dll1);
	//Dll2 = FPlatformProcess::GetDllHandle(*dll2);
	//if (!Dll1 || !Dll2)
	//{
	//	UE_LOG(LogTemp, Warning, TEXT("Failed to load PDF library."));
	//}
#endif
}

void FSimpleKafkaModule::ShutdownModule()
{
	//FPlatformProcess::FreeDllHandle(Dll1);
	//FPlatformProcess::FreeDllHandle(Dll2);
	//Dll1 = nullptr;
	//Dll2 = nullptr;
}

#undef LOCTEXT_NAMESPACE
	
IMPLEMENT_MODULE(FSimpleKafkaModule, SimpleKafka)