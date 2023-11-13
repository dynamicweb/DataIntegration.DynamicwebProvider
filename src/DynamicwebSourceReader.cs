using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System.Data.SqlClient;

namespace Dynamicweb.DataIntegration.Providers.DynamicwebProvider;

public class DynamicwebSourceReader : BaseSqlReader, ISourceReader
{
    public DynamicwebSourceReader(Mapping mapping, SqlConnection connection) : base(mapping, connection) { }
}
