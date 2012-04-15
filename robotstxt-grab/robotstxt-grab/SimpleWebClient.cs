using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace robotstxt_grab
{
  internal static class SimpleWebClient
  {
    public static SimpleWebResponse ExecuteGet(string domain)
    {
      var ret = new SimpleWebResponse();
      var uri = new Uri("http://" + domain + "/robots.txt");
      var req = (HttpWebRequest)WebRequest.Create(uri);

      req.Method = "GET";
      req.KeepAlive = false;

      var resp = (HttpWebResponse)req.GetResponse();
      var stream = new StreamReader(resp.GetResponseStream(), Encoding.GetEncoding("ISO-8859-1"));

      ret.Body = stream.ReadToEnd();
      stream.Close();

      var headers = string.Empty;
      foreach (string header in resp.Headers)
      {
        headers += string.Format("{0}: {1}\r\n", header, resp.Headers[header]);
      }
      ret.Headers = headers;

      return ret;
    }
  }
}
