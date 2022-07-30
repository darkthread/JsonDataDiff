using Newtonsoft.Json;

//args = new string[] { @".\data\source.json", @".\data\target.json", "PK1,PK2", "IgnoreCol1,IgnoreCol2" };
if (args.Length < 3)
{
    Console.WriteLine("Syntax: JsonDataDiff source.json target.json pkCol1,pkCol2,pkCol3");
    Console.WriteLine("Syntax: JsonDataDiff source.json target.json pkCol1,pkCol2 ingoredCol1,ignoreCol2");
    return;
}
var srcPath = args[0];
var dstPath = args[1];
var pkCols = args[2].Split(',');
string[] ignoredCols = args.Length > 3 ? args[3].Split(',') : Array.Empty<string>();
try
{
    var src = JsonConvert.DeserializeObject<List<IDictionary<string, object>>>(File.ReadAllText(srcPath));
    var dst = JsonConvert.DeserializeObject<List<IDictionary<string, object>>>(File.ReadAllText(dstPath));
    var report = Compare(pkCols, src, dst, ignoredCols);
    var color = ConsoleColor.White;
    var values = string.Empty;
    foreach (var entry in report)
    {
        switch (entry.Action)
        {
            case "INSERT":
                color = ConsoleColor.Green;
                values = string.Join(",", entry.Values.Select(o => $"{o.Key}=[{o.Value}]").ToArray());
                break;
            case "DELETE":
                color = ConsoleColor.Magenta;
                values = "Key=" + entry.PrimKey;
                break;
            case "UPDATE":
                color = ConsoleColor.Cyan;
                values = string.Join(",", entry.Values.Select(o => $"{o.Key}:{o.Value}").ToArray());
                break;
        }
        Console.ForegroundColor = color;
        Console.WriteLine($"*** {entry.Action} / {entry.PrimKey} ***");
        Console.ForegroundColor = ConsoleColor.White;
        Console.WriteLine("  " + values);
        Console.ResetColor();
    }
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"*** ERROR ***");
    Console.ResetColor();
    Console.WriteLine(ex.ToString());
}

static DiffEntry[] Compare(string[] primKeys, List<IDictionary<string, object>> src, List<IDictionary<string, object>> dst, string[] ignoredColNames)
{
    string[] colNames = src.First().Keys.ToArray();
    if (dst.Any() && src.First().Keys.Count != dst.First().Keys.Count) throw new ArgumentException("src 與 dst 欄位數不同");
    Func<IDictionary<string, object>, string> GetPKString = (d) => string.Join("|", primKeys.Select(o => d[o]).ToArray());
    Func<object, string> Dump = (o) => {
        if (o == null) return "null";
        if  (o is DateTime) return ((DateTime)o).ToString("yyyy-MM-dd HH:mm:ss").Replace(" 00:00:00", "");
        return o.ToString();
    };
    //產生新增資訊
    Func<IDictionary<string, object>, DiffEntry> GenInsertEntry = (d) =>
    {
        return new DiffEntry
        {
            Action = "INSERT",
            PrimKey = GetPKString(d),
            Values = colNames.Where(o => !ignoredColNames.Contains(o)).ToDictionary(c => c, c => Dump(d[c]))
        };
    };
    //產生刪除資訊
    Func<IDictionary<string, object>, DiffEntry> GenDeleteEntry = (d) =>
    {
        return new DiffEntry
        {
            Action = "DELETE",
            PrimKey = GetPKString(d),
            Values = null
        };
    };
    //比對兩值是否相等
    Func<object, object, bool> AreEqual = (a, b) =>
    {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a is decimal && b is decimal) return a.Equals(b);
        return a.ToString() == b.ToString();
    };
    //產生異動資訊
    Func<IDictionary<string, object>, IDictionary<string, object>, DiffEntry> GenUpdateScript = (source, dest) =>
    {
        var updColNames = new List<string>(); //需要更動的欄位
                var changes = new Dictionary<string, string>();
        colNames
            .Where(colName => !ignoredColNames.Contains(colName))
            .Where(colName => !AreEqual(source[colName], dest[colName])).ToList()
            .ForEach(colName =>
            {
                changes.Add(colName, $"[{dest[colName]}] -> [{source[colName]}]");
            });
        if (!changes.Any())
            return null; //若無欄位要更新，忽略
                return new DiffEntry
        {
            Action = "UPDATE",
            PrimKey = GetPKString(source),
            Values = changes
        };
    };

    //以Primary Key欄位字串為Key，轉成Dictinoary
    var srcDict = src.ToDictionary(o => GetPKString(o), o => o);
    var dstDict = dst.ToDictionary(o => GetPKString(o), o => o);
    //找出來源有但目的沒有的，新增
    var entries = new List<DiffEntry>();
    srcDict.Keys.Except(dstDict.Keys).ToList().ForEach(pk =>
    {
        entries.Add(GenInsertEntry(srcDict[pk]));
    });
    //目的有來源無，刪除之
    dstDict.Keys.Except(srcDict.Keys).ToList().ForEach(pk =>
    {
        entries.Add(GenDeleteEntry(dstDict[pk]));
    });
    //兩邊都存在者，比對是否有異動
    srcDict.Keys.Intersect(dstDict.Keys).ToList().ForEach(pk =>
    {
        var entry = GenUpdateScript(srcDict[pk], dstDict[pk]);
        if (entry != null) entries.Add(entry);
    });
    return entries.ToArray();
}

public class DiffEntry
{
    public string Action { get; set; }
    public string PrimKey { get; set; }
    public IDictionary<string, string> Values { get; set; }
}