using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace robotstxt_grab
{
  class Program
  {
    static void Main(string[] args)
    {
      if (args.Length == 2 && args[0] == "/import")
      {
        var file = args[1];

        if (System.IO.File.Exists(file))
        {
          //create parser and load database
          var imp = new Importer(file);
          imp.Import();
        }
        else
        {
          //nope
          Console.WriteLine("File specified doesn't exist.");
        }
      }
      else if (args.Length == 1 && args[0] == "/index")
      {
        var idx = new Indexer(5);
        idx.Index();
      }

      Console.WriteLine("Press [enter] to exit...");
      Console.ReadLine();
    }
  }
}
