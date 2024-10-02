using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.DynamicwebProvider;

[AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("Dynamicweb Provider"), AddInDescription("Dynamicweb provider"), AddInIgnore(false), AddInUseParameterOrdering(true)]
public class DynamicwebProvider : BaseSqlProvider, IParameterOptions, IParameterVisibility
{
    protected Schema Schema;
    protected bool IsFirstJobRun = true;
    private List<DynamicwebBulkInsertDestinationWriter> Writers = new List<DynamicwebBulkInsertDestinationWriter>();
    private AssortmentHandler AssortmentHandler = null;

    protected string Server;
    protected string Username;
    protected string Password;
    protected string Catalog;
    protected string ManualConnectionString;

    private string _sqlConnectionString;
    protected string SqlConnectionString
    {
        get
        {
            if (!string.IsNullOrEmpty(_sqlConnectionString))
                return _sqlConnectionString;

            if (!string.IsNullOrEmpty(ManualConnectionString))
                return ManualConnectionString;

            //else return constructed connectionString;
            if (string.IsNullOrEmpty(Server) || (!(SourceServerSSPI | DestinationServerSSPI) && (string.IsNullOrEmpty(Username) || string.IsNullOrEmpty(Password))))
            {
                return "";
            }
            return "Data Source=" + Server + ";Initial Catalog=" + Catalog + (SourceServerSSPI | DestinationServerSSPI ? ";Integrated Security=SSPI" : string.Empty) + ";User Id=" + Username + ";Password=" + Password + ";";
        }
        set { _sqlConnectionString = value; }
    }

    public DynamicwebProvider()
    {
        if (string.IsNullOrEmpty(UserKeyField))
            UserKeyField = "AccessUserUserName";
    }

    public DynamicwebProvider(string connectionString)
    {
        SqlConnectionString = connectionString;
        connection = new SqlConnection(SqlConnectionString);
    }

    #region HideParameters
    [AddInParameter("Source server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string SourceServer { get; set; }
    [AddInParameter("Destination server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string DestinationServer
    {
        get { return Server; }
        set { Server = value; }
    }
    [AddInParameter("Use integrated security to connect to source server"), AddInParameterEditor(typeof(Extensibility.Editors.YesNoParameterEditor), ""), AddInParameterGroup("hidden")]
    public bool SourceServerSSPI
    {
        get;
        set;
    }
    [AddInParameter("Use integrated security to connect to destination server"), AddInParameterEditor(typeof(Extensibility.Editors.YesNoParameterEditor), ""), AddInParameterGroup("hidden")]
    public bool DestinationServerSSPI
    {
        get;
        set;
    }
    [AddInParameter("Sql source server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string SourceUsername
    {
        get { return Username; }
        set { Username = value; }
    }
    [AddInParameter("Sql destination server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string DestinationUsername
    {
        get { return Username; }
        set { Username = value; }
    }
    [AddInParameter("Sql source server password"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string SourcePassword
    {
        get { return Password; }
        set { Password = value; }
    }
    [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string DestinationPassword
    {
        get { return Password; }
        set { Password = value; }
    }
    [AddInParameter("Sql source database"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string SourceDatabase
    {
        get { return Catalog; }
        set { Catalog = value; }
    }
    [AddInParameter("Sql source connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string SourceConnectionString
    {
        get { return ManualConnectionString; }
        set { ManualConnectionString = value; }
    }
    [AddInParameter("Sql destination connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string DestinationConnectionString
    {
        get { return ManualConnectionString; }
        set { ManualConnectionString = value; }
    }
    [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("hidden")]
    public string DestinationDatabase
    {
        get { return Catalog; }
        set { Catalog = value; }
    }
    #endregion

    protected SqlConnection connection;
    protected SqlConnection Connection
    {
        get { return connection ??= (SqlConnection)Database.CreateConnection(); }
        set { connection = value; }
    }

    protected string defaultLanguage = null;
    [AddInParameter("Default Language"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;"), AddInParameterGroup("Destination"), AddInParameterOrder(10)]
    public string DefaultLanguage
    {
        get
        {
            if (defaultLanguage == null)
            {
                defaultLanguage = Ecommerce.Services.Languages.GetDefaultLanguageId();
            }
            return defaultLanguage;
        }
        set
        {
            defaultLanguage = value;
        }
    }

    [AddInParameter("Shop"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;"), AddInParameterGroup("Destination"), AddInParameterOrder(20)]
    public string Shop { get; set; }

    [AddInParameter("Insert only new records"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Inserts new records present in the source, but does not update existing records"), AddInParameterGroup("Destination"), AddInParameterOrder(23)]
    public bool InsertOnlyNewRecords { get; set; }

    [AddInParameter("Update only existing records"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When this option is ON the imported rows are updated but not inserted. When OFF rows are updated and inserted"), AddInParameterGroup("Destination"), AddInParameterOrder(25)]
    public bool UpdateOnlyExistingRecords { get; set; }

    [AddInParameter("Deactivate missing products"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When ON missing products are deactivated. When OFF no action is taken"), AddInParameterGroup("Destination"), AddInParameterOrder(30)]
    public bool DeactivateMissingProducts { get; set; }

    [AddInParameter("Remove missing rows after import in the destination tables only"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes rows not present in the import source - excluding related tabled"), AddInParameterGroup("Destination"), AddInParameterOrder(35)]
    public bool RemoveMissingAfterImportDestinationTablesOnly { get; set; }

    [AddInParameter("Remove missing rows after import"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes rows not present in the import source - including related tables. This option takes precedence. When Delete incoming rows is ON, this option is ignored"), AddInParameterGroup("Destination"), AddInParameterOrder(40)]
    public bool RemoveMissingAfterImport { get; set; }

    [AddInParameter("Delete incoming rows"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes existing rows present in the import source. When Delete incoming rows is ON, the following options are skipped: Update only existing records, Deactivate missing products, Remove missing rows after import, Delete products / groups for languages included in input, Hide deactivated products"), AddInParameterGroup("Destination"), AddInParameterOrder(50)]
    public bool DeleteIncomingItems { get; set; }

    [AddInParameter("Delete products/groups for languages included in input"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes products and groups only from the languages included in the import. If Delete incoming rows is ON, Delete products / groups for languages included in input is skipped"), AddInParameterGroup("Destination"), AddInParameterOrder(60)]
    public bool DeleteProductsAndGroupForSpecificLanguage { get; set; }

    [AddInParameter("Discard duplicates"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When ON, duplicate rows are skipped"), AddInParameterGroup("Destination"), AddInParameterOrder(70)]
    public bool DiscardDuplicates { get; set; }

    [AddInParameter("Hide deactivated products"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When Deactivate missing products is ON, this option hides the deactivated products. If Delete incoming rows is ON, Hide deactivated products is skipped. If Deactivate missing products is OFF, Hide deactivated products is skipped"), AddInParameterGroup("Destination"), AddInParameterOrder(80)]
    public bool HideDeactivatedProducts { get; set; }

    [Obsolete("Use Job.RepositoriesIndexSettings")]
    public string RepositoriesIndexUpdate { get; set; }

    [Obsolete("Use the Job.ServiceCacheSettings.Services to clear wanted caches at the end of the job.")]
    public bool DisableCacheClearing { get; set; }

    [AddInParameter("Persist successful rows and skip failing rows"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Checking this box allows the activity to do partial imports by skipping problematic records and keeping the succesful ones"), AddInParameterGroup("Destination"), AddInParameterOrder(100)]
    public bool SkipFailingRows { get; set; }

    /// <summary>
    /// This property is used to remove rows from the EcomGroupProductRelationsTable, but only for the products that are being imported.
    /// It can be set in the job settings xml file in the config section
    /// </summary>
    public virtual bool PartialUpdate { get; set; }

    public string UserKeyField { get; set; }

    public override void SaveAsXml(XmlTextWriter xmlTextWriter)
    {
        xmlTextWriter.WriteElementString("RemoveMissingAfterImport", RemoveMissingAfterImport.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("RemoveMissingAfterImportDestinationTablesOnly", RemoveMissingAfterImportDestinationTablesOnly.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DeactivateMissingProducts", DeactivateMissingProducts.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("UpdateOnlyExistingRecords", UpdateOnlyExistingRecords.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("InsertOnlyNewRecords", InsertOnlyNewRecords.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DeleteIncomingItems", DeleteIncomingItems.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("SqlConnectionString", SqlConnectionString);
        xmlTextWriter.WriteElementString("Shop", Shop);
        xmlTextWriter.WriteElementString("DeleteProductsAndGroupForSpecificLanguage", DeleteProductsAndGroupForSpecificLanguage.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DefaultLanguage", DefaultLanguage);
        xmlTextWriter.WriteElementString("RepositoriesIndexUpdate", RepositoriesIndexUpdate);
        xmlTextWriter.WriteElementString("DiscardDuplicates", DiscardDuplicates.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("HideDeactivatedProducts", HideDeactivatedProducts.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("SkipFailingRows", SkipFailingRows.ToString(CultureInfo.CurrentCulture));
        GetSchema().SaveAsXml(xmlTextWriter);
    }

    public DynamicwebProvider(XmlNode xmlNode)
    {
        RemoveMissingAfterImport = false;
        RemoveMissingAfterImportDestinationTablesOnly = false;
        DeleteIncomingItems = false;
        DeleteProductsAndGroupForSpecificLanguage = false;
        DiscardDuplicates = false;
        HideDeactivatedProducts = false;
        AssortmentHandler = new AssortmentHandler(null, Logger);

        foreach (XmlNode node in xmlNode.ChildNodes)
        {
            switch (node.Name)
            {
                case "SqlConnectionString":
                    if (node.HasChildNodes)
                    {
                        SqlConnectionString = node.FirstChild.Value;
                        Connection = new SqlConnection(SqlConnectionString);
                    }
                    break;
                case "Schema":
                    Schema = new Schema(node);
                    break;
                case "RemoveMissingAfterImport":
                    if (node.HasChildNodes)
                    {
                        RemoveMissingAfterImport = node.FirstChild.Value == "True";
                    }
                    break;
                case "RemoveMissingAfterImportDestinationTablesOnly":
                    if (node.HasChildNodes)
                    {
                        RemoveMissingAfterImportDestinationTablesOnly = node.FirstChild.Value == "True";
                    }
                    break;
                case "DeleteIncomingItems":
                    if (node.HasChildNodes)
                    {
                        DeleteIncomingItems = node.FirstChild.Value == "True";
                    }
                    break;
                case "DeactivateMissingProducts":
                    if (node.HasChildNodes)
                    {
                        DeactivateMissingProducts = node.FirstChild.Value == "True";
                    }
                    break;
                case "UpdateOnlyExistingRecords":
                    if (node.HasChildNodes)
                    {
                        UpdateOnlyExistingRecords = node.FirstChild.Value == "True";
                    }
                    break;
                case "InsertOnlyNewRecords":
                    if (node.HasChildNodes)
                    {
                        InsertOnlyNewRecords = node.FirstChild.Value == "True";
                    }
                    break;
                case "Shop":
                    if (node.HasChildNodes)
                    {
                        Shop = node.FirstChild.Value;
                    }
                    break;
                case "UserKeyField":
                    if (node.HasChildNodes)
                    {
                        UserKeyField = node.FirstChild.Value;
                    }
                    break;
                case "DeleteProductsAndGroupForSpecificLanguage":
                    if (node.HasChildNodes)
                    {
                        DeleteProductsAndGroupForSpecificLanguage = node.FirstChild.Value == "True";
                    }
                    break;
                case "DefaultLanguage":
                    DefaultLanguage = node.HasChildNodes ? node.FirstChild.Value : "";
                    break;
                case "RepositoriesIndexUpdate":
                    if (node.HasChildNodes)
                    {
                        RepositoriesIndexUpdate = node.FirstChild.Value;
                    }
                    break;
                case "DiscardDuplicates":
                    if (node.HasChildNodes)
                    {
                        DiscardDuplicates = node.FirstChild.Value == "True";
                    }
                    break;
                case "HideDeactivatedProducts":
                    if (node.HasChildNodes)
                    {
                        HideDeactivatedProducts = node.FirstChild.Value == "True";
                    }
                    break;
                case "SkipFailingRows":
                    if (node.HasChildNodes)
                    {
                        SkipFailingRows = node.FirstChild.Value == "True";
                    }
                    break;
            }
        }
    }
    public override string ValidateDestinationSettings()
    {
        return "";
    }
    public override string ValidateSourceSettings()
    {
        return null;
    }

    public override void UpdateSourceSettings(ISource source)
    {
        DynamicwebProvider newProvider = (DynamicwebProvider)source;
        Shop = newProvider.Shop;
        UserKeyField = newProvider.UserKeyField;
        DeactivateMissingProducts = newProvider.DeactivateMissingProducts;
        DefaultLanguage = newProvider.DefaultLanguage;
        UpdateOnlyExistingRecords = newProvider.UpdateOnlyExistingRecords;
        InsertOnlyNewRecords = newProvider.InsertOnlyNewRecords;
        HideDeactivatedProducts = newProvider.HideDeactivatedProducts;
        SqlConnectionString = newProvider.SqlConnectionString;
        ManualConnectionString = newProvider.ManualConnectionString;
        RemoveMissingAfterImport = newProvider.RemoveMissingAfterImport;
        RemoveMissingAfterImportDestinationTablesOnly = newProvider.RemoveMissingAfterImportDestinationTablesOnly;
        Username = newProvider.Username;
        Password = newProvider.Password;
        Server = newProvider.Server;
        SourceServerSSPI = newProvider.SourceServerSSPI;
        DestinationServerSSPI = newProvider.DestinationServerSSPI;
        Catalog = newProvider.Catalog;
        DiscardDuplicates = newProvider.DiscardDuplicates;
        SkipFailingRows = newProvider.SkipFailingRows;
    }

    public override void UpdateDestinationSettings(IDestination destination)
    {
        DynamicwebProvider newProvider = (DynamicwebProvider)destination;
        UpdateSourceSettings(newProvider);
        DeleteIncomingItems = newProvider.DeleteIncomingItems;
        DeleteProductsAndGroupForSpecificLanguage = newProvider.DeleteProductsAndGroupForSpecificLanguage;
        DiscardDuplicates = newProvider.DiscardDuplicates;
        HideDeactivatedProducts = newProvider.HideDeactivatedProducts;
        SkipFailingRows = newProvider.SkipFailingRows;
    }

    public override Schema GetOriginalSourceSchema()
    {
        Schema result = GetSqlSourceSchema(Connection, null);
        //set key for AccessUserTable
        if (UserKeyField != null)
        {
            Column keyColumn = result.GetTables().Find(t => t.Name == "AccessUser").Columns.Find(c => c.Name == UserKeyField);
            if (keyColumn != null)
                keyColumn.IsPrimaryKey = true;
        }

        //Set key for other tables that are missing keys in the database
        var table = result.GetTables().FirstOrDefault(t => t.Name == "Ecom7Tree");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "id").IsPrimaryKey = true;
        }
        if (result.GetTables().Exists(t => t.Name.Contains("Ipaper")))
        {
            UpdateIPaperTables(result);
        }
        table = result.GetTables().Find(t => t.Name == "Statv2SessionBot");
        if (table != null)
            table.Columns.Find(c => c.Name == "Statv2SessionID").IsPrimaryKey = true;
        table = result.GetTables().Find(t => t.Name == "Statv2UserAgents");
        if (table != null)
            table.Columns.Find(c => c.Name == "Statv2UserAgentsID").IsPrimaryKey = true;

        //For EcomProducts Remove ProductAutoID column from schema
        Table ecomProductsTable = result.GetTables().Find(t => t.Name == "EcomProducts");
        if (ecomProductsTable != null)
        {
            ecomProductsTable.Columns.RemoveAll(c => c.Name == "ProductAutoID");
        }
        Table ecomAssortmentPermissionsTable = result.GetTables().Find(t => t.Name == "EcomAssortmentPermissions");
        if (ecomAssortmentPermissionsTable != null)
        {
            ecomAssortmentPermissionsTable.AddColumn(new SqlColumn(("AssortmentPermissionCustomerNumber"), typeof(string), SqlDbType.NVarChar, ecomAssortmentPermissionsTable, -1, false, false, true));
            ecomAssortmentPermissionsTable.AddColumn(new SqlColumn(("AssortmentPermissionExternalID"), typeof(string), SqlDbType.NVarChar, ecomAssortmentPermissionsTable, -1, false, false, true));
        }
        return result;
    }

    public override Schema GetSchema()
    {
        Schema ??= GetOriginalSourceSchema();
        return Schema;
    }

    public override void OverwriteSourceSchemaToOriginal()
    {
        Schema = GetOriginalSourceSchema();
    }

    public override void OverwriteDestinationSchemaToOriginal()
    {
        Schema = GetOriginalSourceSchema();
    }

    public override bool RunJob(Job job)
    {
        if (IsFirstJobRun)
        {
            OrderTablesByConstraints(job, Connection);
        }
        SqlTransaction sqlTransaction = null;
        Dictionary<string, object> sourceRow = null;
        Exception exception = null;
        bool deactivateMissingProducts = false;
        try
        {
            ReplaceMappingConditionalsWithValuesFromRequest(job);
            if (Connection.State != ConnectionState.Open)
                Connection.Open();

            foreach (Mapping mapping in job.Mappings)
            {
                if (mapping.Active)
                {
                    var columnMappings = MappingExtensions.ReplaceKeyColumnsWithAutoIdIfExists(mapping);
                    Logger.Log("Starting import to temporary table for " + mapping.DestinationTable.Name + ".");
                    using (var reader = job.Source.GetReader(mapping))
                    {
                        DynamicwebBulkInsertDestinationWriter writer = null;
                        if (IsFirstJobRun)
                        {
                            bool? optionValue = mapping.GetOptionValue("RemoveMissingAfterImport");
                            bool removeMissingAfterImport = optionValue.HasValue ? optionValue.Value : RemoveMissingAfterImport;
                            optionValue = mapping.GetOptionValue("DiscardDuplicates");
                            bool discardDuplicates = optionValue.HasValue ? optionValue.Value : DiscardDuplicates;
                            optionValue = mapping.GetOptionValue("DeactivateMissingProducts");
                            deactivateMissingProducts = optionValue.HasValue ? optionValue.Value : DeactivateMissingProducts;

                            if (!string.IsNullOrEmpty(defaultLanguage))
                            {
                                string destinationColumnNameForLanguageId = MappingExtensions.GetLanguageIdColumnName(mapping.DestinationTable.Name);
                                if (!string.IsNullOrEmpty(destinationColumnNameForLanguageId) && !columnMappings.Any(obj => obj.Active && obj.DestinationColumn.Name.Equals(destinationColumnNameForLanguageId, StringComparison.OrdinalIgnoreCase)))
                                {
                                    Column randomColumn = mapping.SourceTable.Columns.First();
                                    var languageColumnMapping = mapping.AddMapping(randomColumn, mapping.DestinationTable.Columns.Find(c => string.Compare(c.Name, MappingExtensions.GetLanguageIdColumnName(mapping.DestinationTable.Name), true) == 0));
                                    languageColumnMapping.ScriptType = ScriptType.Constant;
                                    languageColumnMapping.ScriptValue = defaultLanguage;
                                }
                            }

                            if (!string.IsNullOrEmpty(Shop))
                            {
                                string destinationColumnNameForShopId = MappingExtensions.GetShopIdColumnName(mapping.DestinationTable.Name);
                                if (!string.IsNullOrEmpty(destinationColumnNameForShopId) && !columnMappings.Any(obj => obj.Active && obj.DestinationColumn.Name.Equals(destinationColumnNameForShopId, StringComparison.OrdinalIgnoreCase)))
                                {
                                    Column randomColumn = mapping.SourceTable.Columns.First();
                                    var shopColumnMapping = mapping.AddMapping(randomColumn, mapping.DestinationTable.Columns.Find(c => string.Compare(c.Name, MappingExtensions.GetShopIdColumnName(mapping.DestinationTable.Name), true) == 0));
                                    shopColumnMapping.ScriptType = ScriptType.Constant;
                                    shopColumnMapping.ScriptValue = Shop;
                                }
                            }

                            writer = new DynamicwebBulkInsertDestinationWriter(mapping, Connection, deactivateMissingProducts, removeMissingAfterImport, Logger, AssortmentHandler, discardDuplicates, RemoveMissingAfterImportDestinationTablesOnly, SkipFailingRows);
                            Writers.Add(writer);
                        }
                        else
                        {
                            writer = Writers.FirstOrDefault(w => w.Mapping.Active && string.Equals(w.Mapping.DestinationTable?.Name, mapping.DestinationTable?.Name, StringComparison.OrdinalIgnoreCase));
                            if (writer == null)
                            {
                                throw new Exception($"Can not find DynamicwebBulkInsertDestinationWriter for the '{mapping.DestinationTable?.Name}' DestinationTable. All used tables must be present in the first Xml file.");
                            }
                        }

                        while (!reader.IsDone())
                        {
                            sourceRow = reader.GetNext();
                            if (ProcessInputRow(sourceRow, mapping))
                            {
                                writer.Write(sourceRow);
                            }
                        }
                        writer.FinishWriting();
                        writer.ClearTableToWrite();
                    }
                    Logger.Log("Finished import to temporary table for " + mapping.DestinationTable.Name + ".");
                }
            }

            sourceRow = null;
            sqlTransaction = Connection.BeginTransaction();

            deactivateMissingProducts = DeactivateMissingProducts;
            var productsWriter = Writers.Where(w => w.Mapping != null && w.Mapping.Active && w.Mapping.DestinationTable != null &&
                w.Mapping.DestinationTable.Name == "EcomProducts").FirstOrDefault();
            if (productsWriter != null)
            {
                bool? value = productsWriter.Mapping.GetOptionValue("DeactivateMissingProducts");
                deactivateMissingProducts = value.HasValue ? value.Value : DeactivateMissingProducts;
            }

            if (deactivateMissingProducts)
                HandleProductsWriter(Writers);

            foreach (DynamicwebBulkInsertDestinationWriter writer in Writers)
            {
                bool? optionValue = writer.Mapping.GetOptionValue("DeleteIncomingItems");
                bool deleteIncomingItems = optionValue.HasValue ? optionValue.Value : DeleteIncomingItems;

                if (writer.RowsToWriteCount > 0)
                {
                    if (deleteIncomingItems)
                    {
                        int rowsAffected = writer.DeleteExistingFromMainTable(Shop, sqlTransaction);
                        if (rowsAffected > 0)
                        {
                            Logger.Log($"The number of deleted rows: {rowsAffected} for the destination {writer.Mapping.DestinationTable.Name} table mapping");
                            TotalRowsAffected += rowsAffected;
                        }
                    }
                    else
                    {
                        writer.AddMappingsToJobThatNeedsToBeThereForMoveToMainTables(job);
                        DynamicwebBulkInsertDestinationWriter.RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables(job);

                        optionValue = writer.Mapping.GetOptionValue("UpdateOnlyExistingRecords");
                        bool updateOnlyExistingRecords = optionValue.HasValue ? optionValue.Value : UpdateOnlyExistingRecords;

                        int rowsAffected = writer.MoveDataToMainTable(sqlTransaction, updateOnlyExistingRecords, InsertOnlyNewRecords);
                        if (rowsAffected > 0)
                        {
                            Logger.Log($"The number of rows affected: {rowsAffected} in the {writer.Mapping.DestinationTable.Name} table");
                            TotalRowsAffected += rowsAffected;
                        }
                    }
                }
                else
                {
                    if (!deleteIncomingItems)
                    {
                        Logger.Log(string.Format("No rows were imported to the table: {0}.", writer.Mapping.DestinationTable.Name));
                    }
                }
            }

            foreach (DynamicwebBulkInsertDestinationWriter writer in Enumerable.Reverse(Writers))
            {
                bool? optionValue = writer.Mapping.GetOptionValue("DeleteIncomingItems");
                bool deleteIncomingItems = optionValue.HasValue ? optionValue.Value : DeleteIncomingItems;

                if (!deleteIncomingItems && writer.RowsToWriteCount > 0)
                {
                    TotalRowsAffected += writer.DeleteExcessFromMainTable(Shop, sqlTransaction, DeleteProductsAndGroupForSpecificLanguage, defaultLanguage, HideDeactivatedProducts);
                }
            }

            if (PartialUpdate)
            {
                //if PartilUpdate property is set, we still want to remove rows from the EcomGroupProductRelationsTable, but only for the products that are being imported
                DynamicwebBulkInsertDestinationWriter groupProductRelationWriter = Writers.Find(w => w.Mapping.DestinationTable != null && w.Mapping.DestinationTable.Name == "EcomGroupProductRelation");
                if (groupProductRelationWriter != null && groupProductRelationWriter.RowsToWriteCount > 0)
                {
                    bool? optionValue = groupProductRelationWriter.Mapping.GetOptionValue("DeleteIncomingItems");
                    bool deleteIncomingItems = optionValue.HasValue ? optionValue.Value : DeleteIncomingItems;
                    if (!deleteIncomingItems)
                    {
                        TotalRowsAffected += groupProductRelationWriter.DeleteExcessGroupProductsRelationsTable();
                    }
                }
            }

            sqlTransaction.Commit();
            AssortmentHandler?.RebuildAssortments();
            MoveRepositoriesIndexToJob(job);
        }
        catch (Exception ex)
        {
            exception = ex;
            string msg = ex.Message;
            string stackTrace = ex.StackTrace;

            Logger?.Error($"Error: {msg.Replace(System.Environment.NewLine, " ")} Stack: {stackTrace.Replace(System.Environment.NewLine, " ")}", ex);
            LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {msg} Stack: {stackTrace}", ex);

            if (ex.Message.Contains("Subquery returned more than 1 value"))
                msg += System.Environment.NewLine + "This error usually indicates duplicates on column that is used as primary key or identity.";

            if (ex.Message.Contains("Bulk copy failures"))
            {
                Logger.Log("Import job failed:");
                BulkCopyHelper.LogFailedRows(Logger, msg);
            }
            else
            {
                if (sourceRow != null)
                    msg += GetFailedSourceRowMessage(sourceRow);
                Logger.Log("Import job failed: " + msg);
            }


            if (sqlTransaction != null)
                sqlTransaction.Rollback();

            TotalRowsAffected = 0;

            return false;
        }
        finally
        {
            foreach (var writer in Writers)
            {
                writer.Close();
            }
            job.Source.Close();
            Connection.Dispose();
            sourceRow = null;
        }
        if (IsFirstJobRun)
        {
            IsFirstJobRun = false;
        }
        return true;
    }

    private void MoveRepositoriesIndexToJob(Job job)
    {
        if (!string.IsNullOrEmpty(RepositoriesIndexUpdate))
        {
            char[] separator = [','];
            // if the provider already have RepositoriesIndexUpdate set, then we move them to the job, and set the add-in to string.empty
            if (job.RepositoriesIndexSettings?.RepositoriesIndexes?.Count == 0)
            {
                job.RepositoriesIndexSettings = new RepositoriesIndexSettings(new Collection<string>([.. RepositoriesIndexUpdate.Split(separator, StringSplitOptions.RemoveEmptyEntries)]));
            }
            RepositoriesIndexUpdate = string.Empty;
            job.Save();
        }
    }

    //Fix for making Products Inactive when checkbox Deactivate Missing Products is on(deleting EcomGroupProductRelation table should be after EcomProducts table)
    private void HandleProductsWriter(List<DynamicwebBulkInsertDestinationWriter> writers)
    {
        DynamicwebBulkInsertDestinationWriter productsWriter = writers.Find(w => w.Mapping.DestinationTable != null && w.Mapping.DestinationTable.Name == "EcomProducts");
        if (productsWriter != null)
        {
            DynamicwebBulkInsertDestinationWriter groupProductRealtionWriter = writers.Find(w => w.Mapping.DestinationTable != null && w.Mapping.DestinationTable.Name == "EcomGroupProductRelation");
            if (groupProductRealtionWriter != null)
            {
                if (writers.IndexOf(groupProductRealtionWriter) < writers.IndexOf(productsWriter))
                {
                    //Put EcomGroupProductRelation writer to end
                    writers.Remove(groupProductRealtionWriter);
                    writers.Add(groupProductRealtionWriter);
                }
            }
        }
    }

    //Required for addin-compatability
    public override string Serialize()
    {
        XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));

        XElement root = new XElement("Parameters");
        document.Add(root);

        root.Add(CreateParameterNode(GetType(), "Connection string", SqlConnectionString));
        root.Add(CreateParameterNode(GetType(), "Deactivate missing products", DeactivateMissingProducts.ToString()));
        root.Add(CreateParameterNode(GetType(), "Update only existing records", UpdateOnlyExistingRecords.ToString()));
        root.Add(CreateParameterNode(GetType(), "Insert only new records", InsertOnlyNewRecords.ToString()));
        root.Add(CreateParameterNode(GetType(), "Remove missing rows after import", RemoveMissingAfterImport.ToString()));
        root.Add(CreateParameterNode(GetType(), "Remove missing rows after import in the destination tables only", RemoveMissingAfterImportDestinationTablesOnly.ToString()));
        root.Add(CreateParameterNode(GetType(), "Delete incoming rows", DeleteIncomingItems.ToString()));
        root.Add(CreateParameterNode(GetType(), "Shop", Shop));
        root.Add(CreateParameterNode(GetType(), "Delete products/groups for languages included in input", DeleteProductsAndGroupForSpecificLanguage.ToString()));
        root.Add(CreateParameterNode(GetType(), "Default Language", DefaultLanguage));
        root.Add(CreateParameterNode(GetType(), "Discard duplicates", DiscardDuplicates.ToString()));
        root.Add(CreateParameterNode(GetType(), "Hide deactivated products", HideDeactivatedProducts.ToString()));
        root.Add(CreateParameterNode(GetType(), "Persist successful rows and skip failing rows", SkipFailingRows.ToString()));
        return document.ToString();
    }

    private void UpdateIPaperTables(Schema schema)
    {
        Table table = schema.GetTables().Find(t => t.Name == "IpaperCategories");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "CategoryID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperLanguageKeys");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "LanguageKeyID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperLanguageKeyValues");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "LanguageKeyValueID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperLanguages");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "LanguageID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperPapers");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "PaperID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperSettingDescriptions");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "DescriptionID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperPages");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "PageID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperSettingGroups");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "GroupID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperSettings");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "SettingID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperSettingSets");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "SetID").IsPrimaryKey = true;
        }
        table = schema.GetTables().Find(t => t.Name == "IpaperSettingTypes");
        if (table != null)
        {
            table.Columns.Find(c => c.Name == "TypeID").IsPrimaryKey = true;
        }
    }

    public override Schema GetOriginalDestinationSchema()
    {
        return GetOriginalSourceSchema();
    }

    public IEnumerable<ParameterOption> GetParameterOptions(string parameterName)
    {
        var options = new List<ParameterOption>();
        if (parameterName == "Shop")
        {
            SqlCommand sqlCommand = new SqlCommand { Connection = Connection };
            if (Connection.State == ConnectionState.Closed)
                Connection.Open();
            SqlDataAdapter languagesDataAdapter = new SqlDataAdapter("select ShopID, ShopName from EcomShops", sqlCommand.Connection);
            new SqlCommandBuilder(languagesDataAdapter);
            DataSet dataSet = new DataSet();
            languagesDataAdapter.Fill(dataSet);
            foreach (DataRow row in dataSet.Tables[0].Rows)
            {
                options.Add(new(row["ShopName"].ToString(), row["shopID"]));
            }
        }
        else if (parameterName == "Product index update")
        {
            options.Add(new("Full", "Full"));
            options.Add(new("Partial", "Partial"));
        }
        else if (parameterName == "Default Language")
        {
            SqlCommand sqlLanguageCommand = new SqlCommand { Connection = Connection };
            if (Connection.State == ConnectionState.Closed)
                Connection.Open();

            SqlDataAdapter languagesDataAdapter = new SqlDataAdapter("select LanguageID, LanguageCode2, LanguageName from EcomLanguages", sqlLanguageCommand.Connection);
            new SqlCommandBuilder(languagesDataAdapter);
            DataSet languageDataSet = new DataSet();
            languagesDataAdapter.Fill(languageDataSet);
            foreach (DataRow row in languageDataSet.Tables[0].Rows)
            {
                options.Add(new(row["LanguageName"].ToString(), row["LanguageID"]));
            }
        }
        else
        {
            var accessuserTable = GetSchema().GetTables().Find(t => t.Name == "AccessUser");
            if (accessuserTable != null)
            {
                foreach (Column column in accessuserTable.Columns)
                {
                    options.Add(new(column.Name, column.Name));
                }
            }
        }
        return options;
    }

    public override void Close()
    {
        Connection.Close();
    }

    public override ISourceReader GetReader(Mapping mapping)
    {
        return new DynamicwebSourceReader(mapping, Connection);
    }

    IEnumerable<string> IParameterVisibility.GetHiddenParameterNames(string parameterName, object parameterValue)
    {
        var result = new List<string>();
        switch (parameterName)
        {
            case "Default Language":
                if (string.IsNullOrEmpty(defaultLanguage) || defaultLanguage.Equals(Ecommerce.Services.Languages.GetDefaultLanguageId(), StringComparison.OrdinalIgnoreCase))
                    result.Add("Default Language");
                break;
            case "Shop":
                if (string.IsNullOrEmpty(Shop))
                    result.Add("Shop");
                break;
        }
        return result;
    }
}
