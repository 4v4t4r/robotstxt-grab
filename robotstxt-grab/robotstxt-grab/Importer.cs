using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace robotstxt_grab
{
  internal class Importer
  {
    private string _file;
    private string _dataPath;
    private string _dataFile;
    private string _connString;
    
    public Importer(string file)
    {
      _file = file;
      _dataPath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Data");
      _dataFile = Path.Combine(_dataPath, "Domains.db");
      _connString = string.Format("data source=\"{0}\"", _dataFile);
    }

    public void Import()
    {
      //see if we hava a database, if not, create one
      if (!File.Exists(_dataFile))
      {
        _InitDatabase();
      }

      var raw = File.ReadLines(_file).GetEnumerator();
      //prime the data, so the first pull gets a value
      raw.MoveNext();

      var count = 0;
      var data = _GetData(raw);

      using (var conn = new SQLiteConnection(_connString))
      {
        conn.Open();
        
        do
        {
          using (var cmd = new SQLiteCommand(conn))
          {
            using (var trans = conn.BeginTransaction())
            {
              cmd.CommandText = "insert into domains (name) values(?)";
              var param = cmd.CreateParameter();
              cmd.Parameters.Add(param);

              foreach (var itm in data)
              {
                param.Value = itm;
                cmd.ExecuteNonQuery();
              }

              trans.Commit();
            }
          }

          count += data.Count();
          Console.WriteLine(string.Format("Imported {0:#,#} items...", count));

          data = _GetData(raw);
        } while (data.Count() > 0);

        conn.Close();
      }
    }

    private IEnumerable<string> _GetData(IEnumerator<string> data)
    {
      var ret = new List<string>();

      for (var i = 0; i < 10000; i++)
      {
        if (data.Current != null)
        {
          ret.Add(data.Current);
        }

        if (!data.MoveNext())
        {
          break;
        }
      }

      return ret;
    }

    private void _InitDatabase()
    {
      if (!Directory.Exists(_dataPath))
      {
        Directory.CreateDirectory(_dataPath);
      }

      SQLiteConnection.CreateFile(_dataFile);
      using (var conn = new SQLiteConnection(_connString))
      {
        conn.Open();
        var cmd = new SQLiteCommand(conn);

        cmd.CommandText = "create table domains(id integer primary key autoincrement, name text, status int default 0)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create unique index idx_domain_name on domains (name)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create index idx_domain_status on domains (status)";
        cmd.ExecuteNonQuery();

        conn.Close();
      }
    }
  }
}
