using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;


namespace EventHubSender
{
    class DatabaseSender
    {
        private static string SQL_CONNECTION_STRING = "Server=DESKTOP-V340A3G\\MSSQLSERVER2017;Initial Catalog=DataVault;Persist Security Info=True;User ID=sa;Password=112233Aa;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=True;Connection Timeout=300;";

        public static void SendMessagesToDatabase(string messages)
        {
            using (var conn = new SqlConnection(SQL_CONNECTION_STRING))
            {
                conn.Open();
                using (var comm = new SqlCommand("InsertJson", conn))
                {
                    comm.CommandType = CommandType.StoredProcedure;
                    comm.Parameters.Add(new SqlParameter("@json", messages));
                    comm.ExecuteReader();
                }
            }
        }
    }
}
