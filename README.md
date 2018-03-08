---
services: Eventhub
platforms: .Net
author: anuchandy
---

# Getting Started with Eventhub - Manage Event Hub Geo Disaster Recovery - in .Net #

          Azure Event Hub sample for managing geo disaster recovery pairing -
            - Create two event hub namespaces
            - Create a pairing between two namespaces
            - Create an event hub in the primary namespace and retrieve it from the secondary namespace
            - Retrieve the pairing connection string
            - Fail over so that secondary namespace become primary.


## Running this Sample ##

To run this sample:

Set the environment variable `AZURE_AUTH_LOCATION` with the full path for an auth file. See [how to create an auth file](https://github.com/Azure/azure-sdk-for-net/blob/Fluent/AUTH.md).

    git clone https://github.com/Azure-Samples/eventhub-dotnet-manage-event-hub-geo-disaster-recovery.git

    cd eventhub-dotnet-manage-event-hub-geo-disaster-recovery

    dotnet restore

    dotnet run

## More information ##

[Azure Management Libraries for C#](https://github.com/Azure/azure-sdk-for-net/tree/Fluent)
[Azure .Net Developer Center](https://azure.microsoft.com/en-us/develop/net/)
If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

---

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.