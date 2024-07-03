using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.DynamicwebProvider;

/// <summary>
/// Dynamicweb Bulk Insert destiation writer
/// </summary>
public class DynamicwebBulkInsertDestinationWriter : BaseSqlWriter, IDestinationWriter, IDisposable
{
    private readonly bool deactivateMissingProducts;
    private AssortmentHandler assortmentHandler;
    protected SqlBulkCopy SqlBulkCopier;
    protected DataSet DataToWrite = new DataSet();
    protected DataTable TableToWrite;
    protected readonly ILogger logger;
    protected readonly bool removeMissingAfterImport;
    protected readonly string tempTablePrefix = "TempTableForSqlProviderImport";
    protected readonly bool discardDuplicates;
    protected DuplicateRowsHandler duplicateRowsHandler;
    protected readonly bool removeMissingAfterImportDestinationTablesOnly;
    protected readonly bool SkipFailingRows;
    public SqlCommand SqlCommand;

    public new Mapping Mapping { get; }

    private DataTable existingUsers;
    private DataTable ExistingUsers
    {
        get
        {
            if (existingUsers == null)
            {
                SqlDataAdapter usersDataAdapter = new SqlDataAdapter("select AccessUserID, AccessUserUserName, AccessUserCustomerNumber, AccessUserExternalID from AccessUser", SqlCommand.Connection);
                new SqlCommandBuilder(usersDataAdapter);
                DataSet dataSet = new DataSet();
                usersDataAdapter.Fill(dataSet);
                existingUsers = dataSet.Tables[0];
            }
            return existingUsers;
        }
    }

    internal int RowsToWriteCount { get; set; }
    private int LastLogRowsCount { get; set; }
    private int SkippedFailedRowsCount { get; set; }

    /// <summary>
    /// Initializes a new instance of the DynamicwebBulkInsertDestinationWriter class.
    /// </summary>        
    /// <param name="mapping">Mapping</param>
    /// <param name="connection">Connection</param>
    /// <param name="shopDeactivateMissingProducts">Deactivate missing products</param>
    /// <param name="deleteMissing">Delete rows from Dynamicweb not present in the import</param>                
    /// <param name="logger">Logger instance</param>        
    public DynamicwebBulkInsertDestinationWriter(Mapping mapping, SqlConnection connection, bool shopDeactivateMissingProducts, bool deleteMissing, ILogger logger, AssortmentHandler assortmentHandler, bool discardDuplicates)
        : this(mapping, connection, shopDeactivateMissingProducts, deleteMissing, logger, assortmentHandler, discardDuplicates, false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DynamicwebBulkInsertDestinationWriter class.
    /// </summary>        
    /// <param name="mapping">Mapping</param>
    /// <param name="connection">Connection</param>
    /// <param name="shopDeactivateMissingProducts">Deactivate missing products</param>
    /// <param name="deleteMissing">Delete rows from Dynamicweb not present in the import</param>
    /// <param name="logger">Logger instance</param>
    /// /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
    public DynamicwebBulkInsertDestinationWriter(Mapping mapping, SqlConnection connection, bool shopDeactivateMissingProducts, bool deleteMissing, ILogger logger, AssortmentHandler assortmentHandler, bool discardDuplicates, bool removeMissingAfterImportDestinationTablesOnly)
        : this(mapping, connection, shopDeactivateMissingProducts, deleteMissing, logger, assortmentHandler, discardDuplicates, removeMissingAfterImportDestinationTablesOnly, false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DynamicwebBulkInsertDestinationWriter class.
    /// </summary>        
    /// <param name="mapping">Mapping</param>
    /// <param name="connection">Connection</param>
    /// <param name="shopDeactivateMissingProducts">Deactivate missing products</param>
    /// <param name="deleteMissing">Delete rows from Dynamicweb not present in the import</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="assortmentHandler">Assortment Handler</param>
    /// <param name="discardDuplicates">Discard Duplicates</param>
    /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
    /// <param name="skipFailingRows">Skip failing rows</param>
    public DynamicwebBulkInsertDestinationWriter(Mapping mapping, SqlConnection connection, bool shopDeactivateMissingProducts, bool deleteMissing, ILogger logger, AssortmentHandler assortmentHandler, bool discardDuplicates, bool removeMissingAfterImportDestinationTablesOnly, bool skipFailingRows)
    {
        this.Mapping = mapping;
        SqlCommand = connection.CreateCommand();
        SqlCommand.CommandTimeout = 1200;
        removeMissingAfterImport = deleteMissing;
        this.removeMissingAfterImportDestinationTablesOnly = removeMissingAfterImportDestinationTablesOnly;
        this.logger = logger;
        tempTablePrefix = $"TempTableForBulkImport{mapping.GetId()}";
        this.discardDuplicates = discardDuplicates;
        SqlBulkCopier = new SqlBulkCopy(connection);
        SqlBulkCopier.DestinationTableName = mapping.DestinationTable.Name + tempTablePrefix;
        SqlBulkCopier.BulkCopyTimeout = 0;
        Initialize();
        //this must be after Initialize() as the connection may be closed in DuplicateRowsHandler->GetOriginalSourceSchema
        if (connection.State != ConnectionState.Open)
            connection.Open();

        deactivateMissingProducts = shopDeactivateMissingProducts;
        this.assortmentHandler = assortmentHandler;
        if (this.assortmentHandler != null && this.assortmentHandler.SqlCommand == null)
        {
            this.assortmentHandler.SqlCommand = SqlCommand;
        }
    }

    /// <summary>
    /// Initializes a new instance of the DynamicwebBulkInsertDestinationWriter class.
    /// </summary>
    /// <param name="mapping">Mapping</param>
    /// <param name="mockSqlCommand">Mock SqlCommand</param>
    /// <param name="shopDeactivateMissingProducts">Deactivate missing products</param>
    /// <param name="deleteMissing">Delete rows from Dynamicweb not present in the import</param>
    public DynamicwebBulkInsertDestinationWriter(Mapping mapping, SqlCommand mockSqlCommand, bool shopDeactivateMissingProducts, bool deleteMissing)
    {
        Mapping = mapping;
        SqlCommand = mockSqlCommand;
        removeMissingAfterImport = deleteMissing;
        logger = LogManager.Current.GetLogger("DataIntegration", "empty.log");
        tempTablePrefix = $"TempTableForBulkImport{mapping.GetId()}";
        discardDuplicates = false;
        Initialize();
        deactivateMissingProducts = shopDeactivateMissingProducts;
        assortmentHandler = new AssortmentHandler(mockSqlCommand, LogManager.Current.GetLogger("DataIntegration", "empty.log"));
    }

    protected new virtual void Initialize()
    {
        List<SqlColumn> destColumns = new List<SqlColumn>();
        var columnMappings = Mapping.GetColumnMappings();
        foreach (ColumnMapping columnMapping in columnMappings.DistinctBy(obj => obj.DestinationColumn.Name))
        {
            destColumns.Add((SqlColumn)columnMapping.DestinationColumn);
        }
        if (Mapping.DestinationTable != null && Mapping.DestinationTable.Name == "EcomAssortmentPermissions")
        {
            if (columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAccessUserID", true) == 0) == null)
                destColumns.Add(new SqlColumn("AssortmentPermissionAccessUserID", typeof(string), SqlDbType.Int, null, -1, false, true, false));
        }
        SQLTable.CreateTempTable(SqlCommand, Mapping.DestinationTable.SqlSchema, Mapping.DestinationTable.Name, tempTablePrefix, destColumns, logger);

        TableToWrite = DataToWrite.Tables.Add(Mapping.DestinationTable.Name + tempTablePrefix);
        foreach (SqlColumn column in destColumns)
        {
            TableToWrite.Columns.Add(column.Name, column.Type);
        }
        if (discardDuplicates)
        {
            duplicateRowsHandler = new DuplicateRowsHandler(logger, Mapping);
        }
    }

    /// <summary>
    /// Writes the specified row.
    /// </summary>
    /// <param name="Row">The row to be written.</param>
    public new void Write(Dictionary<string, object> row)
    {
        DataRow dataRow = TableToWrite.NewRow();
        var columnMappings = Mapping.GetColumnMappings();
        switch (Mapping.DestinationTable.Name)
        {
            case "EcomAssortmentPermissions":
                if (columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAssortmentID", true) == 0) != null &&
                    row[columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAssortmentID", true) == 0).SourceColumn.Name] != DBNull.Value &&
                    !string.IsNullOrEmpty(row[columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAssortmentID", true) == 0).SourceColumn.Name] as string))
                {
                    string assortmentID = (string)row[columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAssortmentID", true) == 0).SourceColumn.Name];
                    List<string> userIDs = new List<string>();
                    if (columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber") != null &&
                        columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber").Active &&
                        row[columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber").SourceColumn.Name] != DBNull.Value)
                    {
                        string userNumber = (string)row[columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber").SourceColumn.Name];
                        if (!string.IsNullOrEmpty(userNumber))
                        {
                            userIDs = ExistingUsers.Select("AccessUserCustomerNumber='" + userNumber.Replace("'", "''") + "'").Select(r => r["AccessUserID"].ToString()).ToList();
                        }
                    }
                    var externalIdmapping = columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionExternalID");
                    if (externalIdmapping != null && externalIdmapping.Active && row[externalIdmapping.SourceColumn.Name] != DBNull.Value)
                    {
                        string externalId = (string)row[externalIdmapping.SourceColumn.Name];
                        if (!string.IsNullOrEmpty(externalId))
                        {
                            userIDs.AddRange(ExistingUsers.Select("AccessUserExternalID='" + externalId.Replace("'", "''") + "'").Select(r => r["AccessUserID"].ToString()));
                        }
                    }
                    var userIdMapping = columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionAccessUserID");
                    if (userIdMapping != null && userIdMapping.Active && row[userIdMapping.SourceColumn.Name] != DBNull.Value)
                    {
                        string id = (string)row[userIdMapping.SourceColumn.Name];
                        if (!string.IsNullOrEmpty(id))
                        {
                            userIDs.AddRange(ExistingUsers.Select("AccessUserID='" + id.Replace("'", "''") + "'").Select(r => r["AccessUserID"].ToString()));
                        }
                    }
                    foreach (string userID in userIDs.Distinct())
                    {
                        DataRow relation = TableToWrite.NewRow();
                        relation["AssortmentPermissionAssortmentID"] = assortmentID;
                        relation["AssortmentPermissionAccessUserID"] = userID;
                        TableToWrite.Rows.Add(relation);
                    }
                    return;
                }
                break;
        }

        var activeColumnMappings = columnMappings.Where(cm => cm.Active);
        foreach (ColumnMapping columnMapping in activeColumnMappings)
        {
            object rowValue = null;
            if (columnMapping.HasScriptWithValue || row.TryGetValue(columnMapping.SourceColumn?.Name, out rowValue))
            {
                object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                if (columnMappings.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() != columnMapping.GetId()))
                {
                    dataRow[columnMapping.DestinationColumn.Name] += dataToRow.ToString();
                }
                else
                {
                    dataRow[columnMapping.DestinationColumn.Name] = dataToRow;
                }
            }
            else
            {
                logger.Info(GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
            }
        }
        if (!discardDuplicates || !duplicateRowsHandler.IsRowDuplicate(activeColumnMappings, Mapping, dataRow, row))
        {
            if (Mapping.DestinationTable.Name == "EcomGroupProductRelation")
            {
                var groupIDColumn = columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "GroupProductRelationGroupID", true) == 0).SourceColumn.Name;
                var productIdColumn = columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "GroupProductRelationProductID", true) == 0).SourceColumn.Name;
                var selectString = "GroupProductRelationGroupID='" + row[groupIDColumn] + "' and GroupProductRelationProductID='" + row[productIdColumn] + "'";
                var columnExists = TableToWrite.Select(selectString);
                if (columnExists.Length < 1)
                {
                    TableToWrite.Rows.Add(dataRow);
                }
            }
            else
            {
                TableToWrite.Rows.Add(dataRow);
            }

            if (assortmentHandler != null)
            {
                assortmentHandler.ProcessAssortments(dataRow, Mapping);
            }

            // if 10k write table to db, empty table
            if (TableToWrite.Rows.Count >= 1000)
            {
                RowsToWriteCount = RowsToWriteCount + TableToWrite.Rows.Count;
                SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, logger);
                RowsToWriteCount = RowsToWriteCount - SkippedFailedRowsCount;
                TableToWrite.Clear();
                if (RowsToWriteCount >= LastLogRowsCount + 10000)
                {
                    LastLogRowsCount = RowsToWriteCount;
                    logger.Log("Added " + RowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
                }
            }
        }
    }

    internal int MoveDataToMainTable(SqlTransaction sqlTransaction, bool updateOnlyExistingRecords, bool insertOnlyNewRecords) =>
        MoveDataToMainTable(Mapping, SqlCommand, sqlTransaction, tempTablePrefix, updateOnlyExistingRecords, insertOnlyNewRecords);

    internal int DeleteExcessFromMainTable(string shop, SqlTransaction transaction, bool deleteProductsAndGroupForSpecificLanguage, string languageId, bool hideDeactivatedProducts)
    {
        SqlCommand.Transaction = transaction;
        int result = 0;
        if ((Mapping.DestinationTable.Name == "EcomProducts" || Mapping.DestinationTable.Name == "EcomGroups") && deleteProductsAndGroupForSpecificLanguage)
        {
            string extraConditions = GetDeleteFromSpecificLanguageExtraCondition(Mapping, tempTablePrefix, languageId);
            var rowsAffected = DeleteExcessFromMainTable(SqlCommand, Mapping, extraConditions, tempTablePrefix, false);
            if (rowsAffected > 0)
            {
                logger.Log($"The number of deleted rows: {rowsAffected} for the destination {Mapping.DestinationTable.Name} table mapping");
                result = (int)rowsAffected;
            }
        }
        else if (Mapping.DestinationTable.Name == "EcomProducts" && deactivateMissingProducts)
        {
            var rowsAffected = DeactivateMissingProductsInMainTable(Mapping, SqlCommand, shop, null, hideDeactivatedProducts);
            if (rowsAffected > 0)
            {
                logger.Log($"The number of the deactivated product rows: {rowsAffected}");
                result = rowsAffected;
            }
        }
        else if ((removeMissingAfterImport || removeMissingAfterImportDestinationTablesOnly) && Mapping.DestinationTable.Name != "AccessUser")
        {
            string extraConditions = GetExtraConditions(Mapping, shop, null);
            var rowsAffected = DeleteExcessFromMainTable(SqlCommand, Mapping, extraConditions, tempTablePrefix, removeMissingAfterImportDestinationTablesOnly);
            if (rowsAffected > 0)
            {
                logger.Log($"The number of deleted rows: {rowsAffected} for the destination {Mapping.DestinationTable.Name} table mapping");
                result = (int)rowsAffected;
            }
        }
        return result;
    }

    internal int DeleteExistingFromMainTable(string shop, SqlTransaction transaction)
    {
        SqlCommand.Transaction = transaction;
        string extraConditions = GetExtraConditions(Mapping, shop, null);
        return DeleteExistingFromMainTable(SqlCommand, Mapping, extraConditions, tempTablePrefix);
    }

    internal void FinishWriting()
    {
        SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, logger);
        if (TableToWrite.Rows.Count != 0)
        {
            RowsToWriteCount = RowsToWriteCount + TableToWrite.Rows.Count - SkippedFailedRowsCount;
            logger.Log("Added " + RowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
        }
    }

    /// <summary>
    /// Returns the sql condition to delete the language specific rows based on the import source languages or
    /// default language id(if it is present)
    /// </summary>
    /// <param name="mapping"></param>
    /// <param name="tempTablePrefix"></param>
    /// <param name="languageId">Null if the language is not selected in the job options</param>
    /// <returns></returns>
    internal static string GetDeleteFromSpecificLanguageExtraCondition(Mapping mapping, string tempTablePrefix, string languageId)
    {
        string ret = string.Empty;
        if (mapping != null && mapping.DestinationTable != null)
        {
            if (mapping.DestinationTable.Name == "EcomProducts")
            {
                if (!string.IsNullOrEmpty(languageId))
                {
                    ret = string.Format(" AND [EcomProducts].[ProductLanguageID] = '{0}' ", languageId);
                }
                else
                {
                    ret = string.Format(" AND [EcomProducts].[ProductLanguageID] IN (SELECT DISTINCT([ProductLanguageID]) FROM [EcomProducts{0}]) ", tempTablePrefix);
                }
            }
            else if (mapping.DestinationTable.Name == "EcomGroups")
            {
                if (!string.IsNullOrEmpty(languageId))
                {
                    ret = string.Format(" AND [EcomGroups].[GroupLanguageID] = '{0}' ", languageId);
                }
                else
                {
                    ret = string.Format(" AND [EcomGroups].[GroupLanguageID] IN (SELECT DISTINCT([GroupLanguageID]) FROM [EcomGroups{0}]) ", tempTablePrefix);
                }
            }
        }
        return ret;
    }

    /// <summary>
    /// Deletes rows not present in the import source. 
    /// This method is called when "PartialUpdate" field settings is set to "true" in the job xml file in the config section.
    /// This method should never be called, unless you're absolutely sure it does what you need.
    /// </summary>        
    internal int DeleteExcessGroupProductsRelationsTable()
    {
        SqlCommand.CommandText = "delete EcomGroupProductRelation from EcomProducts" + tempTablePrefix + " join ecomgroupproductrelation on EcomProducts" + tempTablePrefix + ".productid=ecomgroupproductrelation.GroupProductRelationProductID where not exists (select * from [dbo].[EcomGroupProductRelation" + tempTablePrefix + "] where [dbo].[EcomGroupProductRelation].[GroupProductRelationProductID]=[GroupProductRelationProductID] and [dbo].[EcomGroupProductRelation].[GroupProductRelationGroupID]=[GroupProductRelationGroupID] )";
        return SqlCommand.ExecuteNonQuery();
    }

    internal void AddMappingsToJobThatNeedsToBeThereForMoveToMainTables(Job job)
    {
        if (job != null)
        {
            if (DataToWrite.Tables.Contains("EcomAssortmentPermissions" + tempTablePrefix) && job.Mappings.Find(m => m.DestinationTable.Name == "EcomAssortmentPermissions") != null)
            {
                Mapping mapping = job.Mappings.Find(m => m.DestinationTable.Name == "EcomAssortmentPermissions");
                if (mapping.GetColumnMappings().Find(cm => string.Compare(cm.DestinationColumn.Name, "AssortmentPermissionAccessUserID", true) == 0) == null)
                {
                    //Source columns are irrelevant, but must be set, so they are set to a random column
                    Column randomColumn = job.Source.GetSchema().GetTables().Where(table => table.Columns.Count > 0).First().Columns.First();
                    mapping.AddMapping(randomColumn, job.Destination.GetSchema().GetTables().Find(t => t.Name == "EcomAssortmentPermissions").Columns.Find(c => string.Compare(c.Name, "AssortmentPermissionAccessUserID", true) == 0), true);
                }
            }
        }
    }

    internal static void RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables(Job job)
    {
        Mapping cleanMapping = job.Mappings.Find(m => m.DestinationTable.Name == "EcomAssortmentPermissions");
        if (cleanMapping != null)
        {
            ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
            columnMapping.RemoveAll(cm => cm.DestinationColumn != null && cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber");
            columnMapping.RemoveAll(cm => cm.DestinationColumn != null && cm.DestinationColumn.Name == "AssortmentPermissionExternalID");
        }
    }

    internal void ClearTableToWrite()
    {
        TableToWrite.Clear();
    }

    /// <summary>
    /// Close writer
    /// </summary>
    public new void Close()
    {
        string tableName = Mapping.DestinationTable.Name + tempTablePrefix;
        SqlCommand.CommandText = $"if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{tableName}') AND type in (N'U')) drop table [{tableName}]";
        SqlCommand.ExecuteNonQuery();
        ((IDisposable)SqlBulkCopier).Dispose();
        if (duplicateRowsHandler != null)
        {
            duplicateRowsHandler.Dispose();
        }
    }

    #region IDisposable Implementation
    protected bool Disposed;

    protected void Dispose(bool disposing)
    {
        lock (this)
        {
            // Do nothing if the object has already been disposed of.
            if (Disposed)
                return;

            if (disposing)
            {
                // Release diposable objects used by this instance here.

                if (DataToWrite != null)
                    DataToWrite.Dispose();
                if (TableToWrite != null)
                    TableToWrite.Dispose();
                if (SqlCommand != null)
                    SqlCommand.Dispose();
            }

            // Release unmanaged resources here. Don't access reference type fields.

            // Remember that the object has been disposed of.
            Disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        // Unregister object for finalization.
        GC.SuppressFinalize(this);
    }
    #endregion
}
