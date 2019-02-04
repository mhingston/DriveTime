using log4net;
using Newtonsoft.Json.Linq;
using System;
using System.Timers;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace DriveTime_Service
{
    class Worker
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Worker));
        private SqlConnection connection;
        private readonly NameValueCollection appSettings;
        private readonly DriveTime driveTime;
        private DataTable pending = new DataTable();

        public Worker(SqlConnection connection, NameValueCollection appSettings, string driveTimeConfig)
        {
            this.connection = connection;
            this.appSettings = appSettings;
            driveTime = new DriveTime(driveTimeConfig);
        }

        private async Task ReconnectAsync()
        {
            if (connection.State != ConnectionState.Open)
            {
                try
                {
                    connection.Close();
                    await connection.OpenAsync();
                }

                catch (Exception error)
                {
                    log.Fatal("Unable to connect to database.", error);
                    connection.Dispose();
                    driveTime.Dispose();
                    Environment.Exit((int)Program.ExitCode.NoDbConnection);
                }
            }
        }

        private async Task GetBatchAsync()
        {
            await ProcessPendingRequestsAsync();
            log.Info("Fetching batch...");
            await ReconnectAsync();

            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.CommandText = "DriveTime_qListPending";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Connection = connection;
                DataTable dataTable = new DataTable();

                try
                {
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                    dataTable.Load(reader);
                }

                catch (Exception error)
                {
                    log.Fatal("Unable to get batch from database.", error);
                    connection.Dispose();
                    driveTime.Dispose();
                    Environment.Exit((int)Program.ExitCode.SqlError);
                }

                pending = dataTable;

                if (dataTable.Rows.Count == 0)
                {
                    log.Info("No pending rows, sleeping...");
                    await Task.Delay(Convert.ToInt32(appSettings["SleepDurationMs"]));
                }
            }
        }

        private async Task ProcessPendingRequestsAsync()
        {
            await ReconnectAsync();

            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "DriveTime_Request_xProcess";
                cmd.Connection = connection;

                try
                {
                    await cmd.ExecuteNonQueryAsync();
                    
                    #if DEBUG
                        log.Debug("Processed pending requests.");
                    #endif
                }

                catch (Exception error)
                {
                    log.Error("Unable to process pending requests.", error);
                }
            }
        }
        
        private async Task InsertAsync(DataRow row, JObject json)
        {
            int requestId = 0;
            await ReconnectAsync();

            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "DriveTime_Request_xInsert";
                cmd.Connection = connection;

                cmd.Parameters.AddWithValue("@DriveTimeID", row["DriveTimeID"]);
                cmd.Parameters.AddWithValue("@Origin", row["Origin"]);
                cmd.Parameters.AddWithValue("@Destination", row["Destination"]);
                cmd.Parameters.AddWithValue("@DriveTime", json["DriveTime"] != null ? Convert.ToInt32(json["DriveTime"]) : (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@StatusText", json["StatusText"].ToString());
                cmd.Parameters.AddWithValue("@Timestamp", DateTime.Now);
                cmd.Parameters.AddWithValue("@ProcessComplete", false);

                try
                {
                    requestId = (int)(await cmd.ExecuteScalarAsync());
                    
                    #if DEBUG
                        log.Debug($"Inserted DriveTime_Request (DriveTime_RequestID: {requestId}).");
                    #endif
                }

                catch (Exception error)
                {
                    log.Error("Unable to insert row into DriveTime_Request.", error);
                }
            }
        }

        public void Start()
        {
            log.Info("Service started.");
            StartAsync();
        }

        public async Task StartAsync()
        {
            await GetBatchAsync();

            foreach (DataRow row in pending.Rows)
            {
                string result = await driveTime.LookupAsync(row["Origin"].ToString(), row["Destination"].ToString());
                JObject json = JObject.Parse(result);
                
                if (json["StatusText"].ToString() != "OK" && json["StatusText"].ToString() != "NOT_FOUND")
                {
                    log.Info($"Invalid response from server: {json["StatusText"].ToString()}");
                    pending.Clear();
                    Suspend();
                    return;
                }

                await InsertAsync(row, json);
                await Task.Delay(Convert.ToInt32(appSettings["DelayBetweenRequestsMs"]));
            }

            pending.Clear();
            await StartAsync();
        }

        public void Stop()
        {
            log.Info("Service stopped.");
            connection.Dispose();
            driveTime.Dispose();
            Environment.Exit((int)Program.ExitCode.Normal);
        }

        private void Suspend()
        {
            log.Info("Sleeping until tomorrow.");
            DateTime now = DateTime.Now;
            DateTime tomorrow = now.AddDays(1);
            double duration = (tomorrow.Date - now).TotalMilliseconds;

            Timer timer = new Timer(duration);
            timer.Elapsed += (Object sender, ElapsedEventArgs e) =>
            {
                timer.Stop();
                timer.Dispose();
                StartAsync();
            };
            timer.AutoReset = false;
            timer.Enabled = true;
        }
    }
}
