using MQTTnet;
using MQTTnet.Packets;
using System.Buffers;

namespace MediaSnapshotToMQTTClientApp
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, MQTT Media snapshot connector tracker!");

            // Construct MQTT client
            var mqttFactory = new MqttClientFactory();

            using var mqttClient = mqttFactory.CreateMqttClient();

            var mqttClientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("192.168.3.196", 31883)
                .WithCleanSession(false)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            // Attach handler for received application messages

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                Console.WriteLine($"### RECEIVED APPLICATION MESSAGE (at {DateTime.Now}) ###");
                Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");

                try
                {
                    byte[] payload = e.ApplicationMessage.Payload.ToArray();

                    if (payload.Length > 0)
                    {
                        var fileName = $"snapshot_{DateTime.Now:yyyyMMdd_HHmmss.fff}.jpg";
                        await File.WriteAllBytesAsync(fileName, payload);
                        Console.WriteLine($"+ Saved snapshot to file: {fileName} - with size {payload.Length} (Content: {e.ApplicationMessage.ContentType})");
                    }
                    else
                    {
                        Console.WriteLine("+ Warning: Received empty payload.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception: '{ex.Message}' for message '{e.ApplicationMessage.ConvertPayloadToString()}'");
                }

                Console.WriteLine();

                RemoveOlderImagesToSaveDiskspace();
            };

            // Connect
            mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None).Wait();

            Console.WriteLine("MQTT client connected.");

            // Subscribe to topic 'azure-iot-operations/data/demo-factory-main-ip-camera-media-asset/demo-factory-main-ip-camera-snapshot-2-mqtt'

            await mqttClient.SubscribeAsync(
                new MqttTopicFilter
                {
                    Topic = "media/asset/161/snapshot/#" //"azure-iot-operations/data/#"
                }
            );

            Console.WriteLine("MQTT client subscribed to topic.");

            Console.WriteLine("Hit a key to exit.");

            Console.WriteLine("First, let's clean up older snapshot files to save disk space...");

            RemoveOlderImagesToSaveDiskspace(true);

            Console.WriteLine("Cleanup complete.");

            Console.ReadKey();
        }

        private static void RemoveOlderImagesToSaveDiskspace(bool onstartup = false)
        {
            // remove all JPEG files older than 60 minute
            var files = Directory.GetFiles(Directory.GetCurrentDirectory(), "snapshot_*.jpg");

            if (onstartup)
            {
                Console.WriteLine($"Found {files.Length} snapshot files in '{Directory.GetCurrentDirectory()}'. These are deleted when older than 60 miniutes");
            }

            foreach (var file in files)
            {
                var creationTime = File.GetCreationTime(file);
                if (creationTime < DateTime.Now.AddMinutes(-60))
                {
                    File.Delete(file);
                    Console.WriteLine($"+ Deleted old snapshot file: {file}");
                }
            }
        }
    }
}
