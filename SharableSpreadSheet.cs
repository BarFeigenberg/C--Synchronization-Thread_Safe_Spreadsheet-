using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;  


namespace SpreadSheet
{
    /*********************************************************************************
     * SharableSpreadSheet  (dead-lock-free revision)
     *********************************************************************************/
    public class SharableSpreadSheet
    {
        /* -------------------------------------------------------------------- */
        private int nRows, nCols;
        private readonly int nUsers;

        private string[,] data;
        private readonly List<Mutex> userMutexes;          // per-cell locks
        private readonly Mutex structMutex = new Mutex();  // guards shape & map
        private readonly Dictionary<(int, int), Mutex> cellMutex;

        /* ----------------------- ctor -------------------------------------- */
        public SharableSpreadSheet(int rows, int cols, int users)
        {
            if (rows <= 0 || cols <= 0) throw new ArgumentOutOfRangeException("size");
            if (users <= 0) throw new ArgumentOutOfRangeException("users");

            nRows = rows; nCols = cols; nUsers = users;
            data = new string[rows, cols];
            userMutexes = new List<Mutex>(users);
            cellMutex = new Dictionary<(int, int), Mutex>(rows * cols);

            for (int i = 0; i < nUsers; i++)
                userMutexes.Add(new Mutex(false, $"UserMutex_{i}"));

            /* balanced random mapping */
            int total = rows * cols, baseSize = total / nUsers, rem = total % nUsers;
            int[] quota = new int[nUsers];
            for (int i = 0; i < nUsers; i++) quota[i] = baseSize + (i < rem ? 1 : 0);

            List<int> avail = Enumerable.Range(0, nUsers).ToList();
            Random rnd = new Random();

            for (int r = 0; r < nRows; r++)
                for (int c = 0; c < nCols; c++)
                {
                    int k = rnd.Next(avail.Count);
                    int idx = avail[k];
                    cellMutex[(r, c)] = userMutexes[idx];
                    if (--quota[idx] == 0) { avail[k] = avail[^1]; avail.RemoveAt(avail.Count - 1); }
                }
        }
        /* ----------------------- helpers ----------------------------------- */
        private void CheckCell(int r, int c,
                       [CallerMemberName] string caller = "")
        {
            if (r < 0 || r >= nRows || c < 0 || c >= nCols)
                throw new ArgumentOutOfRangeException(
                    $"[{caller}] tried cell ({r},{c}) but sheet is " +
                    $"{nRows}×{nCols} (valid rows 0..{nRows - 1}, cols 0..{nCols - 1})");
        }
        private Mutex GetMutex(int r, int c) => cellMutex[(r, c)];

        private List<Mutex> CollectLocks(IEnumerable<(int, int)> cells)
        {
            HashSet<Mutex> set = new();
            foreach (var (r, c) in cells) set.Add(GetMutex(r, c));
            var list = set.ToList();
            list.Sort((a, b) => a.GetHashCode().CompareTo(b.GetHashCode()));
            return list;
        }
        private static void LockAll(IEnumerable<Mutex> ms) { foreach (var m in ms) m.WaitOne(); }
        private static void UnlockAll(IEnumerable<Mutex> ms) { foreach (var m in ms.Reverse()) m.ReleaseMutex(); }

        private static bool EqMaybe(string a, string b, bool cs)
        {
            if (cs) return a == b;
            if (a == null || b == null) return a == b;
            return a.ToLower() == b.ToLower();
        }

        /* ------------------- single-cell ops ------------------------------- */
        public string getCell(int r, int c)
        {
            CheckCell(r, c);
            var m = GetMutex(r, c); m.WaitOne();
            try { return data[r, c]; }
            finally { m.ReleaseMutex(); }
        }
        public void setCell(int r, int c, string s)
        {
            CheckCell(r, c);
            var m = GetMutex(r, c); m.WaitOne();
            try { data[r, c] = s; }
            finally { m.ReleaseMutex(); }
        }

        /* ------------------- long readers (all wrapped) -------------------- */
        public Tuple<int, int> searchString(string str)
        {
            structMutex.WaitOne();
            try
            {
                for (int r = 0; r < nRows; r++)
                    for (int c = 0; c < nCols; c++)
                    {
                        var m = GetMutex(r, c); m.WaitOne();
                        try { if (data[r, c] == str) return Tuple.Create(r, c); }
                        finally { m.ReleaseMutex(); }
                    }
                return Tuple.Create(-1, -1);
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public int searchInRow(int row, string str)
        {
            if (row < 0 || row >= nRows) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                for (int c = 0; c < nCols; c++)
                {
                    var m = GetMutex(row, c); m.WaitOne();
                    try { if (data[row, c] == str) return c; }
                    finally { m.ReleaseMutex(); }
                }
                return -1;
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public int searchInCol(int col, string str)
        {
            if (col < 0 || col >= nCols) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                for (int r = 0; r < nRows; r++)
                {
                    var m = GetMutex(r, col); m.WaitOne();
                    try { if (data[r, col] == str) return r; }
                    finally { m.ReleaseMutex(); }
                }
                return -1;
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public Tuple<int, int> searchInRange(int c1, int c2, int r1, int r2, string str)
        {
            if (r1 < 0 || r2 >= nRows || c1 < 0 || c2 >= nCols || r1 > r2 || c1 > c2)
                throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                for (int r = r1; r <= r2; r++)
                    for (int c = c1; c <= c2; c++)
                    {
                        var m = GetMutex(r, c); m.WaitOne();
                        try { if (data[r, c] == str) return Tuple.Create(r, c); }
                        finally { m.ReleaseMutex(); }
                    }
                return Tuple.Create(-1, -1);
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public Tuple<int, int>[] findAll(string str, bool cs)
        {
            structMutex.WaitOne();
            try
            {
                List<Tuple<int, int>> res = new();
                for (int r = 0; r < nRows; r++)
                    for (int c = 0; c < nCols; c++)
                    {
                        var m = GetMutex(r, c); m.WaitOne();
                        try { if (EqMaybe(data[r, c], str, cs)) res.Add(Tuple.Create(r, c)); }
                        finally { m.ReleaseMutex(); }
                    }
                return res.ToArray();
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public void setAll(string oldS, string newS, bool cs)
        {
            structMutex.WaitOne();
            try
            {
                var targets = new List<(int, int)>();
                for (int r = 0; r < nRows; r++)
                    for (int c = 0; c < nCols; c++)
                    {
                        var m = GetMutex(r, c); m.WaitOne();
                        try { if (EqMaybe(data[r, c], oldS, cs)) targets.Add((r, c)); }
                        finally { m.ReleaseMutex(); }
                    }
                var locks = CollectLocks(targets); LockAll(locks);
                try { foreach (var (r, c) in targets) data[r, c] = newS; }
                finally { UnlockAll(locks); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public void exchangeRows(int r1, int r2)
        {
            if (r1 == r2) return;
            if (r1 < 0 || r1 >= nRows || r2 < 0 || r2 >= nRows) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                var cells = new List<(int, int)>();
                for (int c = 0; c < nCols; c++) { cells.Add((r1, c)); cells.Add((r2, c)); }
                var locks = CollectLocks(cells); LockAll(locks);
                try
                {
                    for (int c = 0; c < nCols; c++)
                        (data[r1, c], data[r2, c]) = (data[r2, c], data[r1, c]);
                }
                finally { UnlockAll(locks); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public void exchangeCols(int c1, int c2)
        {
            if (c1 == c2) return;
            if (c1 < 0 || c1 >= nCols || c2 < 0 || c2 >= nCols) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                var cells = new List<(int, int)>();
                for (int r = 0; r < nRows; r++) { cells.Add((r, c1)); cells.Add((r, c2)); }
                var locks = CollectLocks(cells); LockAll(locks);
                try
                {
                    for (int r = 0; r < nRows; r++)
                        (data[r, c1], data[r, c2]) = (data[r, c2], data[r, c1]);
                }
                finally { UnlockAll(locks); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        /* ---------------- addRow / addCol (writers) ------------------------ */
        public void addRow(int after)
        {
            if (after < 0 || after >= nRows) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();                 // ==== writer takes struct lock
            try
            {
                var rowLocks = new HashSet<Mutex>();
                for (int r = after; r < nRows; r++)
                    for (int c = 0; c < nCols; c++)
                        if (rowLocks.Count < nUsers) rowLocks.Add(GetMutex(r, c));

                var list = rowLocks.ToList();
                list.Sort((a, b) => a.GetHashCode().CompareTo(b.GetHashCode()));
                LockAll(list);
                try
                {
                    /* expand data array */
                    string[,] newData = new string[nRows + 1, nCols];
                    for (int r = 0; r <= after; r++)
                        for (int c = 0; c < nCols; c++) newData[r, c] = data[r, c];
                    for (int r = after + 1; r < nRows; r++)
                        for (int c = 0; c < nCols; c++) newData[r + 1, c] = data[r, c];
                    data = newData;

                    /* map new cells */
                    Random rnd = new Random();
                    for (int c = 0; c < nCols; c++)
                        cellMutex[(after + 1, c)] = userMutexes[rnd.Next(nUsers)];

                    /* shift keys for rows below */
                    for (int r = after + 1; r < nRows; r++)
                        for (int c = 0; c < nCols; c++)
                        {
                            var oldKey = (r, c); var newKey = (r + 1, c);
                            cellMutex[newKey] = cellMutex[oldKey];
                        }
                    nRows++;
                }
                finally { UnlockAll(list); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public void addCol(int after)
        {
            if (after < 0 || after >= nCols) throw new ArgumentOutOfRangeException();
            structMutex.WaitOne();
            try
            {
                var colLocks = new HashSet<Mutex>();
                for (int c = after; c < nCols; c++)
                    for (int r = 0; r < nRows; r++)
                        if (colLocks.Count < nUsers) colLocks.Add(GetMutex(r, c));

                var list = colLocks.ToList();
                list.Sort((a, b) => a.GetHashCode().CompareTo(b.GetHashCode()));
                LockAll(list);
                try
                {
                    /* expand data */
                    string[,] newData = new string[nRows, nCols + 1];
                    for (int r = 0; r < nRows; r++)
                        for (int c = 0; c <= after; c++) newData[r, c] = data[r, c];
                    for (int r = 0; r < nRows; r++)
                        for (int c = after + 1; c < nCols; c++) newData[r, c + 1] = data[r, c];
                    data = newData;

                    /* map new cells */
                    Random rnd = new Random();
                    for (int r = 0; r < nRows; r++)
                        cellMutex[(r, after + 1)] = userMutexes[rnd.Next(nUsers)];

                    /* shift keys */
                    for (int r = 0; r < nRows; r++)
                        for (int c = after + 1; c < nCols; c++)
                        {
                            var oldKey = (r, c); var newKey = (r, c + 1);
                            cellMutex[newKey] = cellMutex[oldKey];
                        }
                    nCols++;
                }
                finally { UnlockAll(list); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        /* ----------------------- load / save ------------------------------- */
        /* completely replace the current save() method ------------------------ */
        public void save(string path)
        {
            structMutex.WaitOne();                 // ← NEW: block long readers / writers
            try
            {
                LockAll(userMutexes);              // grab every cell-mutex
                try
                {
                    using StreamWriter sw = new(path);

                    sw.WriteLine($"{nRows}\t{nCols}");
                    for (int r = 0; r < nRows; r++)
                    {
                        string[] row = new string[nCols];
                        for (int c = 0; c < nCols; c++)
                        {
                            string cell = data[r, c] ?? "";
                            row[c] = cell.Replace("\t", "\\t");
                        }
                        sw.WriteLine(string.Join('\t', row));
                    }
                }
                finally { UnlockAll(userMutexes); }
            }
            finally { structMutex.ReleaseMutex(); }   // ← NEW
        }

        public void load(string path)
        {
            structMutex.WaitOne();                // serialize with long readers
            try
            {
                LockAll(userMutexes);             // full consistency
                try
                {
                    string[] lines = File.ReadAllLines(path);
                    if (lines.Length == 0) throw new Exception("empty file");

                    string[] dim = lines[0].Split('\t');
                    int newR = int.Parse(dim[0]);
                    int newC = int.Parse(dim[1]);

                    string[,] newData = new string[newR, newC];

                    for (int r = 0; r < newR; r++)
                    {
                        string[] parts = lines[r + 1].Split('\t');
                        for (int c = 0; c < newC; c++)
                        {
                            string cell = c < parts.Length ? parts[c] : "";
                            newData[r, c] = cell.Replace("\\t", "\t");
                        }
                    }

                    /* swap in new data */
                    data = newData;
                    nRows = newR;
                    nCols = newC;

                    /* rebuild or overwrite mapping WITHOUT clearing */
                    int idx = 0;
                    for (int r = 0; r < nRows; r++)
                        for (int c = 0; c < nCols; c++)
                        {
                            cellMutex[(r, c)] = userMutexes[idx];
                            idx = (idx + 1) % nUsers;
                        }
                }
                finally { UnlockAll(userMutexes); }
            }
            finally { structMutex.ReleaseMutex(); }
        }

        public Tuple<int, int> getSize() => Tuple.Create(nRows, nCols);
    }
}
