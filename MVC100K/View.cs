using MovieLens.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MovieLens.Views
{
    public static class ReportView
    {
        public static void SaveCSV(string name, List<ReportRow> rows, string dir)
        {
            var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var file = Path.Combine(dir, $"{name}_{ts}.csv");

            var sb = new StringBuilder();
            sb.AppendLine("Rank,MovieId,Title,AverageRating,Count");
            int rank = 1;
            foreach (var r in rows)
                sb.AppendLine($"{rank++},{r.MovieId},\"{r.Title}\",{r.Avg:F2},{r.Count}");

            File.WriteAllText(file, sb.ToString(), Encoding.UTF8);
            Console.WriteLine($"✅ Report saved: {file}");
        }
    }
}
