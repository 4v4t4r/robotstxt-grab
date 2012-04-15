using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Simple.Data;

namespace robotstxt_grab
{
  internal class Indexer
  {
    private const int THREADS = 1;
    
    private object _lock = new object();
    private int _done;
    private int _batch;
    private string _dataPath;
    private string _resultFile;
    private string _domainFile;
    private string _connString;
    private dynamic _results;
    private dynamic _domains;

    public Indexer(int batch)
    {
      //batch size might be off slightly because of threading issues - for right now I don't care
      _batch = batch;
      _dataPath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Data");
      _resultFile = Path.Combine(_dataPath, "Results.db");
      _domainFile = Path.Combine(_dataPath, "Domains.db");
      _connString = string.Format("data source=\"{0}\"", _resultFile);
    }

    public void Index()
    {
      //see if we hava a database, if not, create one
      if (!File.Exists(_resultFile))
      {
        _InitDatabase();
      }

      _domains = Database.OpenFile(_domainFile);
      _results = Database.OpenFile(_resultFile);

      for (var i = 0; i < THREADS; i++)
      {
        var th = new Thread(_Index);
        th.IsBackground = true;
        th.Name = "_Index_" + th.ManagedThreadId;
        th.Start();
      }

      //refactor this, nasty, nasty hack
      do
      {
        Thread.Sleep(100);
      } while (_done < THREADS);
    }

    private void _Index()
    {
      do
      {
        //get the next item
        var domain = _GetNextItem();

        if (domain != null)
        {
          try
          {
            var resp = SimpleWebClient.ExecuteGet(domain);
            _results.Results.Insert(Name: domain, Robots: resp.Body, Headers: resp.Headers);

            _MarkItemDone(domain);
          }
          catch (Exception ex)
          {
            Console.WriteLine(ex.ToString());
            _MarkItemFailed(domain);
          }
        }

        _batch--;
      } while (_batch > 0);

      _done += 1;
    }

    private string _GetNextItem()
    {
      string ret = null;

      lock (_lock)
      {
        var obj = _domains.Domains.FindByStatus(Status: 0);

        if (obj != null)
        {
          ret = obj.Name;
          _domains.Domains.UpdateByName(Name: ret, Status: 1);
        }
      }

      return ret;
    }

    private void _MarkItemDone(string name)
    {
      _domains.Domains.UpdateByName(Name: name, Status: 2);
      Console.WriteLine("COMPLETE: " + name);
    }

    private void _MarkItemFailed(string name)
    {
      _domains.Domains.UpdateByName(Name: name, Status: 3);
      Console.WriteLine("FAILED: " + name);
    }

    private void _InitDatabase()
    {
      if (!Directory.Exists(_dataPath))
      {
        Directory.CreateDirectory(_dataPath);
      }

      SQLiteConnection.CreateFile(_resultFile);
      using (var conn = new SQLiteConnection(_connString))
      {
        conn.Open();
        var cmd = new SQLiteCommand(conn);

        cmd.CommandText = "create table results(id integer primary key autoincrement, name text, retrieved timestamp default current_timestamp, robots text, headers text)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create index idx_domain_name on domains (name)";
        cmd.ExecuteNonQuery();

        conn.Close();
      }
    }
  }
}
