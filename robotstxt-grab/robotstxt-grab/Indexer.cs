using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
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
    private const int THREADS = 20;
    
    private object _lock = new object();
    private int _done;
    private int _batch;
    private string _dataPath;
    private string _resultFile;
    private string _domainFile;
    private string _connString;
    private dynamic _results;
    private dynamic _domains;
    private int _complete;
    private TimeSpan _totalTime;

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
      string status = string.Empty;

      do
      {
        var sw = Stopwatch.StartNew();
        //get the next item
        var domain = _GetNextItem();

        if (domain != null)
        {
          try
          {
            var resp = SimpleWebClient.ExecuteGet(domain);
            _results.Results.Insert(Name: domain, Robots: resp.Body, Headers: resp.Headers);

            _MarkItemDone(domain);
            status = "OK";
          }
          catch (Exception ex)
          {
            _MarkItemFailed(domain, ex);
            status = "FAIL";
          }
        }

        sw.Stop();
        _totalTime = _totalTime.Add(sw.Elapsed);
        _complete++;

        Console.WriteLine(string.Format("{0}:\t[Completed: {1} / {2}\tTime: {3}s\tAvg: {4:0}ms]\t{5}", 
          status, _complete, _batch, sw.ElapsedMilliseconds / 1000, _totalTime.TotalSeconds / _complete, domain));
      } while (_complete + THREADS < _batch);

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

          lock (_lock)
          {
            _domains.Domains.UpdateByName(Name: ret, Status: 1);  
          }
        }
      }

      return ret;
    }

    private void _MarkItemDone(string name)
    {
      lock (_lock)
      {
        _domains.Domains.UpdateByName(Name: name, Status: 2);
      }
    }

    private void _MarkItemFailed(string name, Exception ex)
    {
      lock (_lock)
      {
        _domains.Domains.UpdateByName(Name: name, Status: 3);
      }
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
