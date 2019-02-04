using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Scriban;

namespace DriveTime_Service
{
    // Example config JSON string
    //{
    //    "apiUrl": "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins={{origin}}&destinations={{destination}}&key={{api_key}}",
    //    "apiKey": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXXX"
    //}
    public class DriveTime
    {
        private readonly JObject config;
        private readonly HttpClient client;

        public DriveTime(string config)
        {
            this.config = JObject.Parse(config);
            client = new HttpClient();
            client.DefaultRequestHeaders.Add("Accept", "application/json");
        }

        public async Task<string> LookupAsync(string origin, string destination)
        {
            Template template = Template.Parse(config["apiUrl"].ToString());
            string ApiUrl = template.Render(new
            {
                Origin = Uri.EscapeUriString(origin),
                Destination = Uri.EscapeUriString(destination),
                ApiKey = Uri.EscapeUriString(config["apiKey"].ToString())
            });

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, ApiUrl);
            JObject result = new JObject();

            using (HttpResponseMessage response = await client.SendAsync(request))
            {
                try
                {
                    string content = await response.Content.ReadAsStringAsync();

                    if (response.StatusCode >= HttpStatusCode.BadRequest)
                    {
                        HttpRequestException error = new HttpRequestException("Bad request");
                        result["StatusText"] = "UNKNOWN_ERROR";
                        throw error;
                    }

                    JContainer body = (JContainer)JsonConvert.DeserializeObject(content);

                    if (body["status"].ToString() == "OK")
                    {
                        if (body["rows"][0]["elements"][0] != null)
                        {
                            if (body["rows"][0]["elements"][0]["status"].ToString() == "OK")
                            {
                                int duration = Convert.ToInt32(body["rows"][0]["elements"][0]["duration"]["value"]);
                                duration = Convert.ToInt32(Math.Round(duration / 60.0)); // minutes
                                result["DriveTime"] = duration;
                                result["StatusText"] = "OK";
                            }

                            else
                            {
                                result["DriveTime"] = null;
                                result["StatusText"] = "NOT_FOUND";
                            }
                        }

                        else
                        {
                            HttpRequestException error = new HttpRequestException("Bad response");
                            result["StatusText"] = "UNKNOWN_ERROR";
                            throw error;
                        }
                    }

                    else
                    {
                        HttpRequestException error = new HttpRequestException("Bad request");
                        result["StatusText"] = body["status"].ToString();
                        throw error;
                    }
                }

                catch (Exception error)
                {
                    result["DriveTime"] = null;
                }
            }

            return result.ToString();
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }
}
