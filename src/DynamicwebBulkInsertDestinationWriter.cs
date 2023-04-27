using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.DataIntegration.Providers.SqlProvider;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.DynamicwebProvider
{
    /// <summary>
    /// Dynamicweb Bulk Insert destiation writer
    /// </summary>
    public class DynamicwebBulkInsertDestinationWriter : SqlDestinationWriter, IDestinationWriter, IDisposable
    {
        private readonly bool deactivateMissingProducts;
        private AssortmentHandler assortmentHandler;

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
            : base(mapping, connection, deleteMissing, logger, $"TempTableForBulkImport{mapping.GetId()}", discardDuplicates, removeMissingAfterImportDestinationTablesOnly, skipFailingRows)
        {
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
            : base(mapping, mockSqlCommand, deleteMissing, LogManager.Current.GetLogger("DataIntegration", "empty.log"), $"TempTableForBulkImport{mapping.GetId()}", false)
        {
            deactivateMissingProducts = shopDeactivateMissingProducts;
            assortmentHandler = new AssortmentHandler(mockSqlCommand, LogManager.Current.GetLogger("DataIntegration", "empty.log"));
        }

        /// <summary>
        /// Writes the specified row.
        /// </summary>
        /// <param name="Row">The row to be written.</param>
        public override void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }

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
                            row[columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber").SourceColumn.Name] != System.DBNull.Value)
                        {
                            string userNumber = (string)row[columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionCustomerNumber").SourceColumn.Name];
                            if (!string.IsNullOrEmpty(userNumber))
                            {
                                userIDs = ExistingUsers.Select("AccessUserCustomerNumber='" + userNumber.Replace("'", "''") + "'").Select(r => r["AccessUserID"].ToString()).ToList();
                            }
                        }
                        var externalIdmapping = columnMappings.Find(cm => cm.DestinationColumn.Name == "AssortmentPermissionExternalID");
                        if (externalIdmapping != null && externalIdmapping.Active && row[externalIdmapping.SourceColumn.Name] != System.DBNull.Value)
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
                if (columnMapping.HasScriptWithValue || row.ContainsKey(columnMapping.SourceColumn.Name))
                {
                    switch (columnMapping.ScriptType)
                    {
                        case ScriptType.None:
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.ConvertInputToOutputFormat(row[columnMapping.SourceColumn.Name]);
                            break;
                        case ScriptType.Append:
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.ConvertInputToOutputFormat(row[columnMapping.SourceColumn.Name]) + columnMapping.ScriptValue;
                            break;
                        case ScriptType.Prepend:
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.ScriptValue + columnMapping.ConvertInputToOutputFormat(row[columnMapping.SourceColumn.Name]);
                            break;
                        case ScriptType.Constant:
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.GetScriptValue();
                            break;
                        case ScriptType.NewGuid:
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.GetScriptValue();
                            break;
                        case ScriptType.Substring:
                            var substringScriptValue = columnMapping.ScriptValue.Split(':');
                            if (substringScriptValue.Count() != 2 || !int.TryParse(substringScriptValue[0], out int startIndex) || (!int.TryParse(substringScriptValue[1], out int length)))
                            {
                                dataRow[columnMapping.DestinationColumn.Name] = row[columnMapping.SourceColumn.Name];
                                logger.Log($"Script value {columnMapping.ScriptValue} defined on {columnMapping.SourceColumn.Name} is incorrect and could not be processed. Inserted raw value without scripting.");
                                continue;
                            }
                            dataRow[columnMapping.DestinationColumn.Name] = columnMapping.ConvertInputToOutputFormat(row[columnMapping.SourceColumn.Name]).ToString().Substring(startIndex, length);
                            break;
                    }
                }
                else
                {
                    logger.Info(BaseDestinationWriter.GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
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
                    rowsToWriteCount = rowsToWriteCount + TableToWrite.Rows.Count;
                    SqlBulkCopierWriteToServer();
                    rowsToWriteCount = rowsToWriteCount - SkippedFailedRowsCount;
                    TableToWrite.Clear();
                    if (rowsToWriteCount >= lastLogRowsCount + 10000)
                    {
                        lastLogRowsCount = rowsToWriteCount;
                        logger.Log("Added " + rowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
                    }
                }
            }
        }

        internal void DeleteExcessFromMainTable(string shop, SqlTransaction transaction, bool deleteProductsAndGroupForSpecificLanguage, string languageId, bool hideDeactivatedProducts)
        {
            SqlCommand.Transaction = transaction;
            if ((Mapping.DestinationTable.Name == "EcomProducts" || Mapping.DestinationTable.Name == "EcomGroups") && deleteProductsAndGroupForSpecificLanguage)
            {
                string extraConditions = GetDeleteFromSpecificLanguageExtraCondition(Mapping, tempTablePrefix, languageId);
                DeleteExcessFromMainTable(Mapping, extraConditions, SqlCommand, tempTablePrefix);
            }
            else if (Mapping.DestinationTable.Name == "EcomProducts" && deactivateMissingProducts)
            {
                DeactivateMissingProductsInMainTable(Mapping, SqlCommand, shop, null, hideDeactivatedProducts);
            }
            else if ((removeMissingAfterImport || removeMissingAfterImportDestinationTablesOnly) && Mapping.DestinationTable.Name != "AccessUser")
            {
                string extraConditions = GetExtraConditions(Mapping, shop, null);
                DeleteExcessFromMainTable(extraConditions);
            }
        }

        internal void DeleteExistingFromMainTable(string shop, SqlTransaction transaction)
        {
            SqlCommand.Transaction = transaction;
            string extraConditions = GetExtraConditions(Mapping, shop, null);
            DeleteExistingFromMainTable(Mapping, extraConditions, SqlCommand, tempTablePrefix);
        }

        /// <summary>
        /// Deletes existing Dynamicweb data not present in the import source
        /// </summary>
        /// <param name="mapping">Mapping to take the Destination table name to delete data from</param>
        /// <param name="extraConditions">Extra where statement to filter data</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        public static void DeleteExcessFromMainTable(Mapping mapping, string extraConditions, SqlCommand sqlCommand)
        {
            DeleteExcessFromMainTable(mapping, extraConditions, sqlCommand, false);
        }

        /// <summary>
        /// Deletes existing Dynamicweb data not present in the import source
        /// </summary>
        /// <param name="mapping">Mapping to take the Destination table name to delete data from</param>
        /// <param name="extraConditions">Extra where statement to filter data</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
        public static void DeleteExcessFromMainTable(Mapping mapping, string extraConditions, SqlCommand sqlCommand, bool removeMissingAfterImportDestinationTablesOnly)
        {
            DeleteExcessFromMainTable(mapping, extraConditions, sqlCommand, $"TempTableForBulkImport{mapping.GetId()}", removeMissingAfterImportDestinationTablesOnly);
        }

        /// <summary>
        /// Deactivates existing products which are missing in the import source
        /// </summary>
        /// <param name="mapping">EcomProducts mapping</param>
        /// <param name="sqlCommand">Command instance to execute the sql update statement</param>
        /// <param name="shop">Shop ID</param>        
        public static void DeactivateMissingProductsInMainTable(Mapping mapping, SqlCommand sqlCommand, string shop, string languageId, bool hideDeactivatedProducts)
        {
            try
            {
                StringBuilder sqlClean = new StringBuilder("update [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "] set ProductActive = 0");
                if (hideDeactivatedProducts)
                {
                    sqlClean.Append(", ProductHidden = 1");
                }
                sqlClean.Append(" where ");
                string extraConditions = GetExtraConditions(mapping, shop, languageId);
                if (extraConditions.Length > 0)
                {
                    if (extraConditions.StartsWith("and "))
                        extraConditions = extraConditions.Substring("and ".Length - 1);
                    sqlClean.Append(extraConditions);
                    sqlClean.Append(" and ");
                }

                sqlClean.Append(" not exists  (select * from [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name +
                                      $"TempTableForBulkImport{mapping.GetId()}] where ");

                var columnMappings = mapping.GetColumnMappings();
                foreach (ColumnMapping columnMapping in columnMappings)
                {
                    if (columnMapping.Active)
                    {
                        SqlColumn column = (SqlColumn)columnMapping.DestinationColumn;
                        if (column.IsKeyColumn(columnMappings))
                        {
                            sqlClean.Append("[" + mapping.DestinationTable.SqlSchema + "].[" +
                                            mapping.DestinationTable.Name + "].[" + column.Name + "]=[" +
                                            column.Name + "] and ");

                        }
                    }
                }
                sqlClean.Remove(sqlClean.Length - 4, 4);
                sqlClean.Append(")");

                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to remove rows from Table [" + mapping.DestinationTable.SqlSchema + "." + mapping.DestinationTable.Name +
                    "] that where not present in source. Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText, ex);
            }
        }

        internal static string GetExtraConditions(Mapping mapping, string shop, string languageId)
        {
            string extraConditions = "";
            if (mapping.DestinationTable.Name == "EcomProducts")
            {
                if (!string.IsNullOrEmpty(shop))
                    extraConditions =
                        "and not exists(select * from EcomGroupProductRelation join EcomShopGroupRelation on EcomGroupProductRelation.GroupProductRelationGroupID=EcomShopGroupRelation.ShopGroupGroupID where EcomGroupProductRelation.GroupProductRelationProductID=EcomProducts.ProductID and EcomShopGroupRelation.ShopGroupShopID<>'" +
                        shop + "')";
                if (!string.IsNullOrEmpty(languageId))
                {
                    extraConditions = extraConditions + "and (EcomProducts.ProductLanguageID='" + languageId + "')";
                }
            }
            else if (mapping.DestinationTable.Name == "EcomGroupProductRelation" && !string.IsNullOrEmpty(shop))
            {
                extraConditions =
                    "and not exists(select * from EcomShopGroupRelation where EcomGroupProductRelation.GroupProductRelationGroupID=EcomShopGroupRelation.ShopGroupGroupID and  EcomShopGroupRelation.ShopGroupShopID<>'" +
                    shop + "')";
            }
            else if (mapping.DestinationTable.Name == "EcomGroups")
            {
                if (!string.IsNullOrEmpty(shop))
                {
                    extraConditions =
                        "and not exists(select * from EcomShopGroupRelation where EcomGroups.groupid=EcomShopGroupRelation.ShopGroupGroupID and  EcomShopGroupRelation.ShopGroupShopID<>'" +
                        shop + "')";
                }
                if (!string.IsNullOrEmpty(languageId))
                {
                    extraConditions = extraConditions + "and (EcomGroups.GroupLanguageID='" + languageId + "')";
                }

            }
            else if (mapping.DestinationTable.Name == "EcomShopGroupRelation" && !string.IsNullOrEmpty(shop))
            {
                extraConditions = " and EcomShopGroupRelation.ShopGroupShopID='" + shop + "'";
            }
            else if (mapping.DestinationTable.Name == "EcomDetails")
            {
                if (!string.IsNullOrEmpty(shop))
                {
                    extraConditions =
                        "and not exists(select * from EcomGroupProductRelation join EcomShopGroupRelation on EcomGroupProductRelation.GroupProductRelationGroupID=EcomShopGroupRelation.ShopGroupGroupID where EcomGroupProductRelation.GroupProductRelationProductID=EcomDetails.DetailProductID and EcomShopGroupRelation.ShopGroupShopID<>'" +
                        shop + "')";
                }
            }
            return extraConditions;
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
        public virtual void DeleteExcessGroupProductsRelationsTable()
        {
            SqlCommand.CommandText = "delete EcomGroupProductRelation from EcomProducts" + tempTablePrefix + " join ecomgroupproductrelation on EcomProducts" + tempTablePrefix + ".productid=ecomgroupproductrelation.GroupProductRelationProductID where not exists (select * from [dbo].[EcomGroupProductRelation" + tempTablePrefix + "] where [dbo].[EcomGroupProductRelation].[GroupProductRelationProductID]=[GroupProductRelationProductID] and [dbo].[EcomGroupProductRelation].[GroupProductRelationGroupID]=[GroupProductRelationGroupID] )";
            SqlCommand.ExecuteNonQuery();
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

        internal void RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables(Job job)
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
    }
}
