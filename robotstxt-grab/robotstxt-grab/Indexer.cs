using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace robotstxt_grab
{
  internal class Indexer
  {
    private const int THREADS = 50;

    private object _lock = new object();
    private object _lockDone = new object();
    private int _done;
    private int _batch;
    private string _dataPath;
    private string _resultFile;
    private string _domainFile;
    private string _resultConnString;
    private string _domainConnString;
    private SQLiteConnection _resultsConn;
    private SQLiteConnection _domainsConn;
    private int _complete;
    private TimeSpan _totalTime;

    public Indexer(int batch)
    {
      //batch size might be off slightly because of threading issues - for right now I don't care
      _batch = batch;
      _dataPath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Data");
      _resultFile = Path.Combine(_dataPath, "Results.db");
      _domainFile = Path.Combine(_dataPath, "Domains.db");
      _resultConnString = string.Format("data source=\"{0}\"", _resultFile);
      _domainConnString = string.Format("data source=\"{0}\"", _domainFile);
    }

    public void Index()
    {
      //see if we hava a database, if not, create one
      if (!File.Exists(_resultFile))
      {
        _InitDatabase();
      }

      _resultsConn = new SQLiteConnection(_resultConnString);
      _resultsConn.Open();
      _domainsConn = new SQLiteConnection(_domainConnString);
      _domainsConn.Open();

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
        string errorMessage = null;
        var domain = _GetNextItem();

        if (domain != null)
        {
          try
          {
            var resp = SimpleWebClient.ExecuteGet(domain);

            using (var cmd = new SQLiteCommand(_resultsConn))
            {
              cmd.CommandText = "insert into results (name, robots, headers) values (@name, @robots, @headers)";

              var nmp = new SQLiteParameter("@name") { Value = domain };
              cmd.Parameters.Add(nmp);
              var rbp = new SQLiteParameter("@robots") { Value = resp.Body };
              cmd.Parameters.Add(rbp);
              var hdp = new SQLiteParameter("@headers") { Value = resp.Headers };
              cmd.Parameters.Add(hdp);

              cmd.ExecuteNonQuery();
            }

            _MarkItemDone(domain);
            status = "OK";
          }
          catch (Exception ex)
          {
            errorMessage = ex.Message;
            _MarkItemFailed(domain, errorMessage);
            status = "FAIL";
          }
        }

        sw.Stop();
        _totalTime = _totalTime.Add(sw.Elapsed);
        _complete++;

        var message = string.Format("{0}:\tCompleted: {1} / {2}\tTime: {3}s\tAvg: {4:0}s\t{5}",
          status, _complete, _batch, sw.ElapsedMilliseconds / 1000, _totalTime.TotalSeconds / _complete, domain);

        if (errorMessage != null)
        {
          message = string.Format("{0} ({1})", message, errorMessage);
        }

        Console.WriteLine(message);
      } while (_complete + THREADS < _batch);

      _done += 1;
    }

    private string _GetNextItem()
    {
      string ret;

      lock (_lock)
      {
        using (var cmd = new SQLiteCommand(_domainsConn))
        {
          cmd.CommandText = "select name from domains where status = 0";
          ret = cmd.ExecuteScalar().ToString();
        }

        if (!string.IsNullOrEmpty(ret))
        {
          _UpdateStatus(ret, 1);
        }
      }

      return ret;
    }

    private void _MarkItemDone(string name)
    {
      _UpdateStatus(name, 2);
    }

    private void _MarkItemFailed(string name, string message)
    {
      _UpdateStatus(name, 3);

      using (var cmd = new SQLiteCommand(_resultsConn))
      {
        cmd.CommandText = "insert into errors (name, message) values (@name, @message)";

        var nmp = new SQLiteParameter("@name") { Value = name };
        cmd.Parameters.Add(nmp);
        var stp = new SQLiteParameter("@message") { Value = message };
        cmd.Parameters.Add(stp);

        cmd.ExecuteNonQuery();
      }
    }

    private void _UpdateStatus(string name, int status)
    {
      using (var cmd = new SQLiteCommand(_domainsConn))
      {
        cmd.CommandText = "update domains set status = @status where name = @name";

        var stp = new SQLiteParameter("@status") {Value = status};
        cmd.Parameters.Add(stp);
        var nmp = new SQLiteParameter("@name") {Value = name};
        cmd.Parameters.Add(nmp);

        cmd.ExecuteNonQuery();
      }
    }

    private void _InitDatabase()
    {
      if (!Directory.Exists(_dataPath))
      {
        Directory.CreateDirectory(_dataPath);
      }

      SQLiteConnection.CreateFile(_resultFile);
      using (var conn = new SQLiteConnection(_resultConnString))
      {
        conn.Open();
        var cmd = new SQLiteCommand(conn);

        cmd.CommandText = "create table results(id integer primary key autoincrement, name text, retrieved timestamp default current_timestamp, robots text, headers text)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create index idx_domain_name on domains (name)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create index idx_domain_status on domains (status)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "create table errors(id integer primary key autoincrement, name text, retrieved timestamp default current_timestamp, message text)";
        cmd.ExecuteNonQuery();

        conn.Close();
      }
    }
  }
}
