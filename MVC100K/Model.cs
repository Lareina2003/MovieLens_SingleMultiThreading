using System;
using System.Collections.Generic;
using System.Linq;

namespace MovieLens.Models
{
    public class Movie
    {
        public int MovieId { get; set; }
        public string Title { get; set; }
        public List<string> Genres { get; set; } = new List<string>();
    }

    public class User
    {
        public int UserId { get; set; }
        public string Gender { get; set; }
        public int Age { get; set; }
        public string Occupation { get; set; }
    }

    public class Rating
    {
        public int UserId { get; set; }
        public int MovieId { get; set; }
        public double Score { get; set; }
    }

    public class ReportRow
    {
        public int MovieId { get; set; }
        public string Title { get; set; }
        public double Avg { get; set; }
        public int Count { get; set; }
    }
}
