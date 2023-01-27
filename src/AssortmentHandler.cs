using Dynamicweb.Core;
using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.Ecommerce.Assortments;
using Dynamicweb.Extensibility.Notifications;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.ProviderHelpers
{
    public class AssortmentHandler
    {
        private Dictionary<string, List<AssortmentInfo>> assortmentsToRebuild = new Dictionary<string, List<AssortmentInfo>>();
        private readonly string assortmentsProductsTempTableName = "AssortmentsProductsTempTable";
        private readonly ILogger logger = null;
        private SqlCommand sqlCommand = null;
        private readonly string CurrentContextLanguageId = Ecommerce.Common.Context.LanguageID;

        public SqlCommand SqlCommand
        {
            get
            {
                return sqlCommand;
            }
            set
            {
                sqlCommand = value;
            }
        }

        public AssortmentHandler(SqlCommand sqlCommand, ILogger logger)
        {
            this.sqlCommand = sqlCommand;
            this.logger = logger;
        }

        #region Assortments rebuild
        public void ProcessAssortments(DataRow dataRow, Mapping mapping)
        {
            if (dataRow != null && mapping.DestinationTable != null)
            {
                string assortmentID = null;
                string assortmentLanguageId = CurrentContextLanguageId;
                string relationID = null;
                AssortmentRelationType relationType = AssortmentRelationType.Product;

                switch (mapping.DestinationTable.Name)
                {
                    case "EcomAssortments":
                        if (dataRow.Table.Columns.Contains("AssortmentRebuildRequired") && dataRow["AssortmentRebuildRequired"] != DBNull.Value && Converter.ToBoolean(dataRow["AssortmentRebuildRequired"]) &&
                            dataRow.Table.Columns.Contains("AssortmentID") && dataRow["AssortmentID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentID"] as string))
                        {
                            assortmentID = (string)dataRow["AssortmentID"];
                            if(dataRow.Table.Columns.Contains("AssortmentLanguageID") && dataRow["AssortmentLanguageID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentLanguageID"] as string))
                            {
                                assortmentLanguageId = (string)dataRow["AssortmentLanguageID"];
                            }
                        }
                        break;
                    case "EcomAssortmentGroupRelations":
                        if (dataRow.Table.Columns.Contains("AssortmentGroupRelationAssortmentID") && dataRow["AssortmentGroupRelationAssortmentID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentGroupRelationAssortmentID"] as string) &&
                            dataRow.Table.Columns.Contains("AssortmentGroupRelationGroupID") && dataRow["AssortmentGroupRelationGroupID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentGroupRelationGroupID"] as string))
                        {
                            assortmentID = (string)dataRow["AssortmentGroupRelationAssortmentID"];
                            relationID = (string)dataRow["AssortmentGroupRelationGroupID"];
                            relationType = AssortmentRelationType.Group;
                        }
                        break;
                    case "EcomAssortmentProductRelations":
                        if (dataRow.Table.Columns.Contains("AssortmentProductRelationAssortmentID") && dataRow["AssortmentProductRelationAssortmentID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentProductRelationAssortmentID"] as string) &&
                            dataRow.Table.Columns.Contains("AssortmentProductRelationProductID") && dataRow["AssortmentProductRelationProductID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentProductRelationProductID"] as string))
                        {
                            assortmentID = (string)dataRow["AssortmentProductRelationAssortmentID"];
                            relationID = (string)dataRow["AssortmentProductRelationProductID"];
                            relationType = AssortmentRelationType.Product;
                        }
                        break;
                    case "EcomAssortmentShopRelations":
                        if (dataRow.Table.Columns.Contains("AssortmentShopRelationAssortmentID") && dataRow["AssortmentShopRelationAssortmentID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentShopRelationAssortmentID"] as string) &&
                            dataRow.Table.Columns.Contains("AssortmentShopRelationShopID") && dataRow["AssortmentShopRelationShopID"] != DBNull.Value && !string.IsNullOrEmpty(dataRow["AssortmentShopRelationShopID"] as string))
                        {
                            assortmentID = (string)dataRow["AssortmentShopRelationAssortmentID"];
                            relationID = (string)dataRow["AssortmentShopRelationShopID"];
                            relationType = AssortmentRelationType.Shop;
                        }
                        break;
                }
                if (!string.IsNullOrEmpty(assortmentID))
                {
                    AssortmentInfo info = new AssortmentInfo();
                    info.AssortmentID = assortmentID;
                    info.RelationID = relationID;
                    info.RelationType = relationType;
                    if (string.IsNullOrEmpty(relationID) //this happens when AssortmentRebuildRequired = true
                        || IsAssortmentToRebuild(info))
                    {
                        if (!assortmentsToRebuild.Keys.Contains(assortmentID))
                        {
                            assortmentsToRebuild.Add(assortmentID, new List<AssortmentInfo>());
                        }
                        if (!string.IsNullOrEmpty(relationID))
                        {
                            assortmentsToRebuild[assortmentID].Add(info);
                        }
                    }
                }
            }
        }

        internal void RebuildAssortments()
        {
            var assortmentsForBuild = new List<Assortment>();
            var failedAssortments = new List<Assortment>();

            if (assortmentsToRebuild.Count > 0)
            {
                CreateAssortmentProductsTempTable();
                InsertAssortmentProductsToTempTable(assortmentsToRebuild);
                UpdateAssortmentsProducts(false);
                Assortment assortment = null;
                string msg = "";

                foreach (string assortmentId in assortmentsToRebuild.Keys)
                {
                    try
                    {
                        assortment = Ecommerce.Services.Assortments.GetAssortmentById(assortmentId);
                        if (assortment is object)
                        {
                            assortmentsForBuild.Add(assortment);
                            Ecommerce.Services.Assortments.BuildAssortment(assortment);
                        }
                    }
                    catch (Exception ex)
                    {
                        msg += string.Format("Error rebulding assortments: {0}. ", ex.Message);
                        if (assortment != null)
                        {
                            failedAssortments.Add(assortment);
                            msg += string.Format("AssortmentID: {0}. ", assortment.ID);
                        }
                    }
                }

                UpdateAssortmentsProducts(true);
                DeleteAssortmentProductsTempTable();

                if (!string.IsNullOrEmpty(msg))
                {
                    logger.Log(msg);
                }
            }
            NotificationManager.Notify(Dynamicweb.Ecommerce.Notifications.Ecommerce.Assortment.AssortmentsBuildFinished,
                                                         new Dynamicweb.Ecommerce.Notifications.Ecommerce.Assortment.AssortmentsBuildFinishedArgs(assortmentsForBuild, failedAssortments));
        }

        private bool IsAssortmentToRebuild(AssortmentInfo info)
        {
            bool rebuild = false;
            if (info is object && !string.IsNullOrEmpty(info.RelationID))
            {
                Assortment assortment = Ecommerce.Services.Assortments.GetAssortmentById(info.AssortmentID);
                if (assortment is object)
                {
                    if (info.RelationType == AssortmentRelationType.Product)
                    {
                        rebuild = !assortment.ProductRelations.Values.Any(relation =>
                            string.Compare(relation.ProductID, info.RelationID, true) == 0);
                    }
                    else if (info.RelationType == AssortmentRelationType.Group)
                    {
                        rebuild = !assortment.GroupRelations.Keys.Any(groupID => string.Compare(groupID, info.RelationID, true) == 0);
                    }
                    else if (info.RelationType == AssortmentRelationType.Shop)
                    {
                        rebuild = !assortment.ShopRelations.Keys.Any(shopID => string.Compare(shopID, info.RelationID, true) == 0);
                    }
                }
            }
            return rebuild;
        }

        private void InsertAssortmentProductsToTempTable(Dictionary<string, List<AssortmentInfo>> assortments)
        {
            if (assortments != null && assortments.Count > 0)
            {
                string sql = string.Format(@"insert into {0}(ProductAutoID) select ProductAutoID from EcomProducts 
                                                  where ProductActive = 1 and ( ProductID in ( ", assortmentsProductsTempTableName);

                string groupIDs = string.Join("','", assortments.SelectMany(v => v.Value).Where(x => x.RelationType == AssortmentRelationType.Group && !string.IsNullOrEmpty(x.RelationID)).Select(i => i.RelationID).Distinct());
                string shopIDs = string.Join("','", assortments.SelectMany(v => v.Value).Where(x => x.RelationType == AssortmentRelationType.Shop && !string.IsNullOrEmpty(x.RelationID)).Select(i => i.RelationID).Distinct());
                if (!string.IsNullOrEmpty(groupIDs) || !string.IsNullOrEmpty(shopIDs))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(sql);
                    sb.Append("select GroupProductRelationProductID from EcomGroupProductRelation ");
                    sb.Append("where ");
                    if (!string.IsNullOrEmpty(shopIDs))
                    {
                        sb.AppendFormat("GroupProductRelationGroupID in( select ShopGroupGroupID from EcomShopGroupRelation where ShopGroupShopID in ( '{0}' )) or ",
                            shopIDs);
                    }
                    if (!string.IsNullOrEmpty(groupIDs))
                    {
                        sb.AppendFormat("GroupProductRelationGroupID in ( '{0}' )", groupIDs);
                    }
                    else
                    {
                        sb.Remove(sb.Length - 4, 3); //remove or
                    }
                    sb.Append(") )");
                    ExecuteInsertAssortmentProductsToTempTable(sb.ToString());
                }

                List<string> productIDs = assortments.SelectMany(v => v.Value).Where(x => x.RelationType == AssortmentRelationType.Product && !string.IsNullOrEmpty(x.RelationID)).Select(i => i.RelationID).Distinct().ToList();
                if (productIDs.Count > 0)
                {
                    int maxProductIDsInQuery = 3000;
                    if (productIDs.Count > maxProductIDsInQuery)
                    {
                        int chunks = productIDs.Count / maxProductIDsInQuery;
                        if (productIDs.Count % maxProductIDsInQuery != 0)
                        {
                            chunks++;
                        }
                        for (int i = 0; i < chunks; i++)
                        {
                            string pds = string.Join("','", productIDs.Skip(i * maxProductIDsInQuery).Take(maxProductIDsInQuery));
                            ExecuteInsertAssortmentProductsToTempTable(string.Format("{0} '{1}' ) )", sql, string.Join("','", productIDs)));
                        }
                    }
                    else
                    {
                        ExecuteInsertAssortmentProductsToTempTable(string.Format("{0} '{1}' ) )", sql, string.Join("','", productIDs)));
                    }
                }
            }
        }

        private void ExecuteInsertAssortmentProductsToTempTable(string sql)
        {
            sqlCommand.CommandText = sql;
            try
            {
                sqlCommand.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error insert assortments products to temp table: {0}. Sql: {1}.", ex.Message, sql));
            }
        }

        private void CreateAssortmentProductsTempTable()
        {
            List<SqlColumn> columns = new List<SqlColumn>();
            columns.Add(new SqlColumn("ProductAutoID", "bigint", null, 0, true, true));
            SQLTable.CreateTempTable(sqlCommand, null, assortmentsProductsTempTableName, null, columns, logger);
        }

        private void DeleteAssortmentProductsTempTable()
        {
            sqlCommand.CommandText = "if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + assortmentsProductsTempTableName + "') AND type in (N'U')) drop table " + assortmentsProductsTempTableName;
            sqlCommand.ExecuteScalar();
        }

        private void UpdateAssortmentsProducts(bool setActive)
        {
            sqlCommand.CommandText = string.Format("update EcomProducts set ProductActive = {0} where ProductAutoID in (select distinct(ProductAutoID) from {1})",
                    Database.SqlBool(setActive), assortmentsProductsTempTableName);
            sqlCommand.ExecuteScalar();
        }
        #endregion Assortments rebuild
    }
}
