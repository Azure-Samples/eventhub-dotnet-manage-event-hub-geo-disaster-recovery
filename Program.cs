// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.EventHubs;
using Azure.ResourceManager.EventHubs.Models;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Samples.Common;


namespace ManageEventHubGeoDisasterRecovery
{
    public class Program
    {
        /**
         * Azure Event Hub sample for managing geo disaster recovery pairing -
         *   - Create two event hub namespaces
         *   - Create a pairing between two namespaces
         *   - Create an event hub in the primary namespace and retrieve it from the secondary namespace
         *   - Retrieve the pairing connection string
         *   - Fail over so that secondary namespace become primary.
         */

         private static ResourceIdentifier? _resourceGroupId = null;

        public static async Task RunSample(ArmClient client)
        {
            string rgName = Utilities.CreateRandomName("rgeh");
            string primaryNamespaceName = Utilities.CreateRandomName("ns");
            string secondaryNamespaceName = Utilities.CreateRandomName("ns");
            string geoDRName = Utilities.CreateRandomName("geodr");
            string eventHubName = Utilities.CreateRandomName("eh");
            bool isFailOverSucceeded = false;
            EventHubsDisasterRecoveryResource pairing = null;

            try
            {
                //============================================================
                // Create resource group for the namespaces and recovery pairings
                //
                SubscriptionResource subscription = await client.GetDefaultSubscriptionAsync();
                ResourceGroupData resourceGroupData = new ResourceGroupData(AzureLocation.SouthCentralUS);
                ResourceGroupResource resourceGroup = (await subscription.GetResourceGroups()
                    .CreateOrUpdateAsync(WaitUntil.Completed, rgName, resourceGroupData)).Value;
                _resourceGroupId = resourceGroup.Id;

                Utilities.Log($"Creating primary event hub namespace {primaryNamespaceName}");

                EventHubsNamespaceData eventHubsNamespaceData = new EventHubsNamespaceData(AzureLocation.SouthCentralUS);
                EventHubsNamespaceResource primaryNamespace = (await resourceGroup.GetEventHubsNamespaces()
                    .CreateOrUpdateAsync(WaitUntil.Completed, primaryNamespaceName, eventHubsNamespaceData)).Value;

                Utilities.Log("Primary event hub namespace created with name: " + primaryNamespace.Data.Name);

                Utilities.Log($"Creating secondary event hub namespace {primaryNamespaceName}");

                EventHubsNamespaceData secondaryNamespaceData = new EventHubsNamespaceData(AzureLocation.NorthCentralUS);
                EventHubsNamespaceResource secondaryNamespace = (await resourceGroup.GetEventHubsNamespaces()
                    .CreateOrUpdateAsync(WaitUntil.Completed, secondaryNamespaceName, secondaryNamespaceData)).Value;

                Utilities.Log("Secondary event hub namespace created with name: " + secondaryNamespace.Data);

                //============================================================
                // Create primary and secondary namespaces and recovery pairing
                //

                Utilities.Log($"Creating geo-disaster recovery pairing {geoDRName}");

                EventHubsDisasterRecoveryData disasterRecoveryData = new EventHubsDisasterRecoveryData()
                {
                    PartnerNamespace = secondaryNamespace.Id
                };
                pairing = (await primaryNamespace.GetEventHubsDisasterRecoveries()
                    .CreateOrUpdateAsync(WaitUntil.Completed, geoDRName, disasterRecoveryData)).Value;

                Utilities.Log($"Created geo-disaster recovery pairing {geoDRName}");

                //============================================================
                // Create an event hub and consumer group in primary namespace
                //

                Utilities.Log("Creating an event hub and consumer group in primary namespace");

                EventHubResource eventHubInPrimaryNamespace = (await primaryNamespace.GetEventHubs()
                    .CreateOrUpdateAsync(WaitUntil.Completed, eventHubName, new EventHubData())).Value;

                EventHubsConsumerGroupData eventHubsConsumerGroupData = new EventHubsConsumerGroupData() { UserMetadata = "sometadata" };
                EventHubsConsumerGroupResource eventHubsConsumerGroup = (await eventHubInPrimaryNamespace.GetEventHubsConsumerGroups()
                    .CreateOrUpdateAsync(WaitUntil.Completed, "consumerGrp1", eventHubsConsumerGroupData)).Value;

                Utilities.Log("Created event hub and consumer group in primary namespace");

                Utilities.Log("Waiting for 80 seconds to allow metadata to sync across primary and secondary");
                Thread.Sleep(80 * 1000); // Wait for syncing to finish

                Utilities.Log("Retrieving the event hubs in secondary namespace");

                EventHubResource eventHubInSecondaryNamespace = await secondaryNamespace.GetEventHubs().GetAsync(eventHubName);

                Utilities.Log("Retrieved the event hubs in secondary namespace");

                //============================================================
                // Retrieving the connection string
                //
                var rules = pairing.GetEventHubsDisasterRecoveryAuthorizationRules().GetAllAsync();
                await foreach (var rule in rules)
                {
                    EventHubsAccessKeys key = await rule.GetKeysAsync();
                    Utilities.Log("Key is: " + key.AliasPrimaryConnectionString);
                }

                Utilities.Log("Initiating fail over");
                await (await secondaryNamespace.GetEventHubsDisasterRecoveryAsync(eventHubName)).Value.FailOverAsync();
                isFailOverSucceeded = true;

                Utilities.Log("Fail over initiated");
            }
            finally
            {
                try
                {
                    try
                    {
                        // It is necessary to break pairing before deleting resource group
                        //
                        Utilities.Log("Pairing breaking");
                        if (pairing != null && !isFailOverSucceeded)
                        {
                            await pairing.BreakPairingAsync();
                        }
                    }
                    catch (Exception ex)
                    {
                        Utilities.Log("Pairing breaking failed:" + ex.Message);
                    }
                    if (_resourceGroupId is not null)
                    {
                        Console.WriteLine($"Deleting Resource Group: {_resourceGroupId}");
                        await client.GetResourceGroupResource(_resourceGroupId).DeleteAsync(WaitUntil.Completed);
                        Console.WriteLine($"Deleted Resource Group: {_resourceGroupId}");
                    }
                }
                catch (NullReferenceException)
                {
                    Utilities.Log("Did not create any resources in Azure. No clean up is necessary");
                }
                catch (Exception ex)
                {
                    Utilities.Log(ex);
                }
            }
        }

        public static async Task Main(string[] args)
        {
            try
            {
                //=================================================================
                // Authenticate
                var credential = new DefaultAzureCredential();

                var subscriptionId = Environment.GetEnvironmentVariable("AZURE_SUBSCRIPTION_ID");
                // you can also use `new ArmClient(credential)` here, and the default subscription will be the first subscription in your list of subscription
                var client = new ArmClient(credential, subscriptionId);

                await RunSample(client);
            }
            catch (Exception ex)
            {
                Utilities.Log(ex);
            }
        }
    }
}
