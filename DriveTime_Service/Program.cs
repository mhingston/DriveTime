using log4net;
using Newtonsoft.Json.Linq;
using Scriban;
using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using Topshelf;

namespace DriveTime_Service
{
    class Program
    {
        public enum ExitCode { Normal = 0, NoDbConnection, SqlError, RequestError };
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["Test"].ConnectionString);
        private static readonly NameValueCollection appSettings = ConfigurationManager.AppSettings;
        private static readonly string path = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase).Replace(@"file:\", "");

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure(new FileInfo(Path.Combine(path, "log4net.config")));
            TopshelfExitCode returnCode = HostFactory.Run(app =>
            {
                app.UseLog4Net();
                app.Service<Worker>(service =>
                {
                    string driveTimeConfig = File.ReadAllText(Path.Combine(path, "config.json"));
                    JObject json = JObject.Parse(driveTimeConfig);

                    Template template = Template.Parse(json["apiKey"].ToString());
                    string apiKey = template.Render(new
                    {
                        ApiKey = appSettings["ApiKey"]
                    });
                    json["apiKey"] = apiKey;
                    driveTimeConfig = json.ToString();

                    service.ConstructUsing(name => new Worker(connection, appSettings, driveTimeConfig));
                    service.WhenStarted(worker => worker.Start());
                    service.WhenStopped(worker => worker.Stop());
                });
                app.StartAutomatically();
                app.RunAsLocalService();
                app.SetServiceName("DriveTime Service");
                app.SetDescription("Checks against the Google Distance Matrix API");

                app.EnableServiceRecovery(recovery =>
                {
                    recovery.RestartService(5);
                });
            });

            int exitCode = (int)Convert.ChangeType(returnCode, returnCode.GetTypeCode());
            Environment.ExitCode = exitCode;
        }
    }
}
