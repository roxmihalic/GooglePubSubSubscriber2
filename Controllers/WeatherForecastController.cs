using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Microsoft.AspNetCore.Mvc;

namespace GooglePubSubSubscriber1.Controllers
{
	[ApiController]
	[Route("[controller]")]
	public class WeatherForecastController : ControllerBase
	{
		public WeatherForecastController()
		{
		}

		[HttpGet]
		public async Task<IEnumerable<string>> Get()
		{
			var messages = await PullMessagesWithCustomAttributesAsync("nmt-infra-development", "sub_two", true);
			var stringMessages = messages.Select(m => System.Text.Encoding.UTF8.GetString(m.Data.ToArray()));
			return stringMessages.ToArray();
		}



		public async Task<List<PubsubMessage>> PullMessagesWithCustomAttributesAsync(string projectId, string subscriptionId, bool acknowledge)
		{
			SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

			SubscriberClient subscriber = await SubscriberClient.CreateAsync(subscriptionName);
			var messages = new List<PubsubMessage>();
			Task startTask = subscriber.StartAsync((PubsubMessage message, CancellationToken cancel) =>
			{
				messages.Add(message);
				string text = System.Text.Encoding.UTF8.GetString(message.Data.ToArray());
				Console.WriteLine($"Message subs2 {message.MessageId}: {text}");
				if (message.Attributes != null)
				{
					foreach (var attribute in message.Attributes)
					{
						Console.WriteLine($"{attribute.Key} = {attribute.Value}");
					}
				}
				return Task.FromResult(acknowledge ? SubscriberClient.Reply.Ack : SubscriberClient.Reply.Nack);
			});
			// Run for 7 seconds.
			await Task.Delay(7000);
			await subscriber.StopAsync(CancellationToken.None);
			// Lets make sure that the start task finished successfully after the call to stop.
			await startTask;
			return messages;
		}
	}
}
