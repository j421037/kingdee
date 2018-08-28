using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kingdee.BOS;
using Kingdee.BOS.Util;
using Kingdee.BOS.App;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Core.Enums;
using Kingdee.BOS.Core.Report;
using Kingdee.BOS.Orm.DataEntity;

using Kingdee.K3.FIN.Core;
using Kingdee.K3.FIN.Core.Parameters;
using Kingdee.K3.FIN.AR.App.Report;
using Kingdee.K3.FIN.App.Core;
using Kingdee.K3.FIN.Core.ARAP.FilterCondition;
using Kingdee.K3.FIN.App.Core.ARAP.AbstractReport;

using Kingdee.K3.FIN.AR.APP.Custom.Report;

using System.ComponentModel;
using System.IO;
using System.Data;

namespace Kingdee.K3.FIN.AR.App.Custom.Report
{
    [Description("应收款汇总表服务端插件")]
    public class CustomerARSumReportService : AbstractDetailReportService
    {

        private List<SqlObject> sqlObjList = new List<SqlObject>();

        private string _checkReportID = "";

        private List<long> ContactList;

        public override void Initialize()
        {
            List<DecimalControlField> decimalControlFieldList = this.SetAmountDigit();
            base.ReportProperty.IsGroupSummary = true;
            base.ReportProperty.DecimalControlFieldList = decimalControlFieldList;
        }
        /**
         * @param filter 过滤条件
         * @param string tableName, 目标临时表名称
         * **/
        public override void BuilderReportSqlAndTempTable(IRptParams filter, string tableName)
        {
            //创建临时表， 用于存放自己的数据
            IDBService dbserver = Kingdee.BOS.App.ServiceHelper.GetService<IDBService>();
            string[] customRptTempTableName = dbserver.CreateTemporaryTableName(this.Context, 1);
            string strTable = customRptTempTableName[0];
            
            //调用基类的方法把初步查询的数据存放到临时表
            base.BuilderReportSqlAndTempTable(filter, strTable);
            List<string> list = new List<string>();
            using (new SessionScope())
            {
                list.AddRange(this.MyBuildSumRptData(tableName, strTable));
                StringBuilder sb = new StringBuilder();
                foreach (string value in list)
                {
                    sb.Append(value);
                }
                File.WriteAllText("C:\\Users\\Administrator\\Desktop\\debug\\debug.sql", sb.ToString());
                DBUtils.ExecuteBatch(base.Context, list, 30);
            }
        }
        /**
         * 拼凑从临时表到目标表的SQL
         * @params bosTableName 目标bos表名
         * @params DetailTmpTable 自定义临时表
         * **/
        protected List<string> MyBuildSumRptData(string bosTableName, string DetailTmpTable)
        {
            List<string> list = new List<string>();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Clear();
            stringBuilder.AppendLine("SELECT max(T1.FCONTACTUNITNUMBER) as FCONTACTUNITNUMBER");
            if (this.FilterCondition.WebAPI)
            {
                stringBuilder.AppendLine(" ,MAX(T1.FCONTACTUNITTYPE) AS FCONTACTUNITYPE ");
                stringBuilder.AppendLine(" ,MAX(T1.FCONTACTUNITID) AS FCONTACTUNITID ");
            }
            stringBuilder.AppendLine(" ,max(T1.FCONTACTUNITNAME) as FCONTACTUNITNAME");
            stringBuilder.AppendLine(" ,max(T1.FCURRENCYFORNAME) as FCURRENCYFORNAME");
            stringBuilder.AppendLine(" ,max(T1.FCURRENCYNAME) as FCURRENCYNAME");
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSORGNAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FBUSINESSORGNAME) as FBUSINESSORGNAME");
            }
            stringBuilder.AppendLine(" ,max(T1.FSETTLEORGNAME) as FSETTLEORGNAME");
            if (this.FilterCondition.GroupbyFields.Contains("FRPORGNAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FRPORGNAME) as FRPORGNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSDEPTNAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FBUSINESSDEPTNAME) as FBUSINESSDEPTNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSGROUPNAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FBUSINESSGROUPNAME) as FBUSINESSGROUPNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSERNAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FBUSINESSERNAME) as FBUSINESSERNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBILLTYPENAME"))
            {
                stringBuilder.AppendLine(" ,max(T1.FBILLTYPENAME) as FBILLTYPENAME");
            }
            stringBuilder.AppendFormat(" ,sum(Case FDetailType When {0} Then T1.FLEFTAMOUNTFOR else 0 End) as FINITAMOUNTFOR", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance));
            stringBuilder.AppendLine(" ,sum(T1.FAMOUNTFOR) as FAMOUNTFOR");
            stringBuilder.AppendLine(" ,sum(T1.FHADIVAMOUNTFOR) as FHADIVAMOUNTFOR");
            stringBuilder.AppendLine(" ,sum(T1.FREALAMOUNTFOR) as FREALAMOUNTFOR");
            stringBuilder.AppendLine(" ,sum(T1.FOFFAMOUNTFOR) as FOFFAMOUNTFOR");
            stringBuilder.AppendFormat(" ,sum(T1.FAMOUNTFOR - T1.FREALAMOUNTFOR - T1.FOFFAMOUNTFOR + Case FDetailType When {0} Then T1.FLEFTAMOUNTFOR else 0 End) as FLEFTAMOUNTFOR", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance));
            stringBuilder.AppendFormat(" ,sum(Case FDetailType When {0} Then T1.FLEFTAMOUNT else 0 End) as FINITAMOUNT", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance));
            stringBuilder.AppendLine(" ,sum(T1.FAMOUNT) as FAMOUNT");
            stringBuilder.AppendLine(" ,sum(T1.FHADIVAMOUNT) as FHADIVAMOUNT");
            stringBuilder.AppendLine(" ,sum(T1.FREALAMOUNT) as FREALAMOUNT");
            stringBuilder.AppendLine(" ,sum(T1.FOFFAMOUNT) as FOFFAMOUNT");
            stringBuilder.AppendFormat(" ,sum(T1.FAMOUNT - T1.FREALAMOUNT - T1.FOFFAMOUNT + Case FDetailType When {0} Then T1.FLEFTAMOUNT else 0 End) as FLEFTAMOUNT", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance));
            stringBuilder.AppendLine(" ,max(T1.FAMOUNTDIGITSFOR) as FAMOUNTDIGITSFOR");
            stringBuilder.AppendLine(" ,max(T1.FAMOUNTDIGITS) as FAMOUNTDIGITS");
            stringBuilder.AppendLine(" ,max(T1.FCURRENCYRECNAME) AS FCURRENCYRECNAME");
            stringBuilder.AppendLine(" ,sum(T1.FREFUNDAMOUNTFOR) as FREFUNDAMOUNTFOR");
            stringBuilder.AppendLine(" ,0 as FNOTETYPE");
            stringBuilder.AppendFormat(" ,'{0}' as FDetailTableName", DetailTmpTable);
            stringBuilder.AppendFormat(" ,{0}", string.Format(this.KSQL_SEQ, this.FilterCondition.GroupbyFields));
            stringBuilder.AppendFormat(" INTO {0}", bosTableName);
            stringBuilder.AppendFormat(" FROM {0} T1", DetailTmpTable);
            stringBuilder.AppendFormat(" Group By {0}", this.FilterCondition.GroupbyFields);
            list.Add(stringBuilder.ToString());
            list.Add(string.Format("Delete From {0} Where FDetailType = {1}", DetailTmpTable, Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance)));
            return list;
        }
        protected override string GetRptTempTableName(Context ctx, DetailReportCondition filterCondition)
		{
			base.Context = ctx;
			if (this.SubSystemID == "AR")
			{
				this.FDebitARAP = "2";
			}
			else
			{
				this.FDebitARAP = "1";
			}
			string tempTableName = CommonFunction.GetTempTableName(ctx);
			if (filterCondition.GroupbyFields == null || string.IsNullOrWhiteSpace(filterCondition.GroupbyFields))
			{
				filterCondition.GroupbyFields = "FCONTACTUNITNUMBER,FCURRENCYFORNAME,FBUSINESSDEPTNAME";
			}
			if (!filterCondition.GroupbyFields.Contains("FCURRENCYFORNAME"))
			{
				filterCondition.GroupbyFields += ",FCURRENCYFORNAME";
			}
			if (!filterCondition.GroupbyFields.Contains("FCONTACTUNITNUMBER") && (string.IsNullOrWhiteSpace(filterCondition.FormId) || filterCondition.FormId == null))
			{
				filterCondition.GroupbyFields += ",FCONTACTUNITNUMBER";
			}
			filterCondition.OrderbyFields = this.SetOrderByString(filterCondition.GroupbyFields, filterCondition.OrderbyFields);
			this.BuildTableData(tempTableName, filterCondition);
			return tempTableName;
		}

		// Token: 0x0600018F RID: 399 RVA: 0x0000F8B0 File Offset: 0x0000DAB0
		protected override string GetRptTempTableNameByHook(Context ctx, DetailReportCondition filterCondition, string strCheckReportID)
		{
			base.Context = ctx;
			this._checkReportID = strCheckReportID;
			if (filterCondition.GroupbyFields == null || string.IsNullOrWhiteSpace(filterCondition.GroupbyFields))
			{
				filterCondition.GroupbyFields = "FCONTACTUNITNUMBER,FCURRENCYNAME";
			}
			filterCondition.OrderbyFields = "FCONTACTUNITNUMBER,FCURRENCYNAME,FDetailType,FDATE,FBUSINESSRANK,FBILLNO";
			string tempTableName = CommonFunction.GetTempTableName(ctx);
			this.BuildTableData(tempTableName, filterCondition);
			return tempTableName;
		}

		// Token: 0x06000190 RID: 400 RVA: 0x0000F908 File Offset: 0x0000DB08
		private void BuildTableData(string tableName, DetailReportCondition filterCondition)
		{
			this.FilterCondition = filterCondition;
			this.ContactList = this.GetLinkSupOrVend();
			using (new SessionScope())
			{
				this.CreateTempTableAndIndex();
				this.BuildData(tableName);
				//this.DeleteTempTable();
			}
		}
        protected override void BuildData(string tableName)
        {
            if (this.CheckReportID.Trim().Length > 0)
            {
                this.BuildDataCheck(tableName);
                return;
            }
            string text = string.Empty;
            if (base.ReportId != null)
            {
                text = base.ReportId.ToUpperInvariant();
            }
            string a;
            if ((a = text) != null)
            {
                if (a == "AP_IVDETAILREPORT")
                {
                    this.BuildAPNoBillData(tableName);
                    return;
                }
                if (a == "AR_IVDETAILREPORT")
                {
                    this.BuildARNoBillData(tableName);
                    return;
                }
            }
            if (this.SubSystemID == "AR")
            {
                this.BuildARData(tableName);
                return;
            }
            this.BuildAPData(tableName);
        }
        private void BuildDataCheck(string tableName)
        {
            string checkReportID;
            if ((checkReportID = this.CheckReportID) != null)
            {
                if (checkReportID == "AP_IVDetailReport")
                {
                    this.BuildAPNoBillData(tableName);
                    return;
                }
                if (!(checkReportID == "AR_IVDetailReport"))
                {
                    return;
                }
                this.BuildARNoBillData(tableName);
            }
        }
        private void BuildAPNoBillData(string tableName)
        {
            this.sqlObjList.Clear();
            this.sqlObjList.Add(base.ARAPIV_RPBill("AP_Payable", AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            this.sqlObjList.Add(base.ARAPIV_IVBill("IV_PURCHASEIC", AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            if (base.FilterCondition.IncludeExpBill)
            {
                this.sqlObjList.Add(base.ARAPIV_IVBillExp("IV_PUREXPINV", AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            }
            this.sqlObjList.Add(base.ARAPIV_RPBill("AP_Payable", AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
            this.sqlObjList.Add(base.ARAPIV_IVBill("IV_PURCHASEIC", AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
            if (base.FilterCondition.IncludeExpBill)
            {
                this.sqlObjList.Add(base.ARAPIV_IVBillExp("IV_PUREXPINV", AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
            }
            this.sqlObjList.AddRange(base.ARAPIV_SumRptData());
            this.sqlObjList.Add(base.ARAPIV_TransTempDataToBosTable(tableName));
            this.sqlObjList.RemoveAll((SqlObject o) => o == null);
            DBUtils.ExecuteBatch(base.Context, this.sqlObjList);

            this.ARAPIV_UpdateRowDataEndBalance(tableName);
        }
        private void BuildARNoBillData(string tableName)
        {
            this.sqlObjList.Clear();
            this.sqlObjList.Add(base.ARAPIV_RPBill("AR_receivable", AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            this.sqlObjList.Add(base.ARAPIV_IVBill("IV_SALESOC", AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            this.sqlObjList.Add(base.ARAPIV_RPBill("AR_receivable", AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
            this.sqlObjList.Add(base.ARAPIV_IVBill("IV_SALESOC", AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
            this.sqlObjList.AddRange(base.ARAPIV_SumRptData());
            this.sqlObjList.Add(base.ARAPIV_TransTempDataToBosTable(tableName));
            this.sqlObjList.RemoveAll((SqlObject o) => o == null);

            

            DBUtils.ExecuteBatch(base.Context, this.sqlObjList);

            this.ARAPIV_UpdateRowDataEndBalance(tableName);
        }

        private void BuildARData(string tableName)
        {
            this.sqlObjList.Clear();
            if (base.FilterCondition.ViewFromSumReport)
            {
                this.sqlObjList.AddRange(base.InsertTmpData_FromSumRpt());
            }
            else
            {
                this.sqlObjList.Add(base.InsertTmpData_AR_OrgEndDate());
                if (base.FilterCondition.OnlyShowPayEvaluate)
                {
                    this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_OutStockReturnBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBillIni(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBillIniReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                }
                else if (base.FilterCondition.OnlyShowPayEvaluate_New)
                {
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceivableBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ContactBal(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceivableBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                }
                else
                {
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceivableBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_OtherRecAbleBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceiveBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance, 0));
                    this.sqlObjList.Add(base.InsertTmpData_AR_RefundBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_Match(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_Match(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_InnerRecClear(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ContactBal(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceivableBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_OtherRecAbleBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_ReceiveBill(AbstractDetailReportService.BusinessTYPEENUM.Amount, 0));
                    this.sqlObjList.Add(base.InsertTmpData_AR_RefundBill(AbstractDetailReportService.BusinessTYPEENUM.Amount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_Match(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_Match(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_InnerRecClear(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    if (base.FilterCondition.IncludePayEvaluate)
                    {
                        this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AR_OutStockReturnBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBillIni(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AR_OutStockBillIniReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    }
                }
                this.sqlObjList.AddRange(this.InsertTmpData_SumRptData());
                this.sqlObjList.AddRange(base.DeleteTmpData_DontShowData());
            }
            this.sqlObjList.Add(this.TransTempDataToBosTable(tableName));
            this.sqlObjList.RemoveAll((SqlObject o) => o == null);
            DBUtils.ExecuteBatch(base.Context, this.sqlObjList);
            this.UpdateRowDataEndBalance(tableName);
        }

        private void BuildAPData(string tableName)
        {
            this.sqlObjList.Clear();
            if (base.FilterCondition.ViewFromSumReport)
            {
                this.sqlObjList.AddRange(base.InsertTmpData_FromSumRpt());
            }
            else
            {
                this.sqlObjList.Add(base.InsertTmpData_AP_OrgEndDate());
                if (base.FilterCondition.OnlyShowPayEvaluate)
                {
                    this.sqlObjList.Add(base.InsertTmpData_AP_INStockBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillIni(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillIniReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_PropertyConvertBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                }
                else if (base.FilterCondition.OnlyShowPayEvaluate_New)
                {
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayableBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_ContactBal(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayableBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                }
                else
                {
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayableBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_OtherPayAbleBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_RefundBill(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_Match(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_ER_Match(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AR_Match(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_InnerPayClear(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_ContactBal(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayableBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_OtherPayAbleBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_PayBill(AbstractDetailReportService.BusinessTYPEENUM.Amount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_RefundBill(AbstractDetailReportService.BusinessTYPEENUM.Amount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_Match(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    this.sqlObjList.Add(base.InsertTmpData_ER_Match(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AR_Match(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    this.sqlObjList.Add(base.InsertTmpData_AP_InnerPayClear(AbstractDetailReportService.BusinessTYPEENUM.ReversedAmount));
                    if (base.FilterCondition.IncludePayEvaluate)
                    {
                        this.sqlObjList.Add(base.InsertTmpData_AP_INStockBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillIni(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AP_INStockBillIniReturn(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                        this.sqlObjList.Add(base.InsertTmpData_AP_PropertyConvertBill(AbstractDetailReportService.BusinessTYPEENUM.RPAmount));
                    }
                }
                this.sqlObjList.AddRange(this.InsertTmpData_SumRptData());
                this.sqlObjList.AddRange(base.DeleteTmpData_DontShowData());
            }
            this.sqlObjList.Add(this.TransTempDataToBosTable(tableName));
            this.sqlObjList.RemoveAll((SqlObject o) => o == null);
            DBUtils.ExecuteBatch(base.Context, this.sqlObjList);
            this.sqlObjList.Clear();
            this.UpdateRowDataEndBalance(tableName);
        }

        /**处理汇总临时数据表**/
        protected override List<SqlObject> InsertTmpData_SumRptData()
        {
            List<SqlObject> list = new List<SqlObject>();
            StringBuilder stringBuilder = new StringBuilder();
            TableFieldTuples tableFieldTuples = new TableFieldTuples();
            List<SqlParam> list2 = new List<SqlParam>();
            bool flag = this.FilterCondition.ShowMaterialDetail || this.FilterCondition.GroupbyFields.Contains("FMaterialNumber");
            tableFieldTuples.Add("FCONTACTUNITTYPE", "max(IsNull(B21.FFormID,' '))");
            tableFieldTuples.Add("FCONTACTUNITID", "max(IsNull(B21.FMASTERID,0))");
            tableFieldTuples.Add("FCONTACTUNITNUMBER", "max(IsNull(B21.FNumber,' ')) as FCONTACTUNITNUMBER");
            tableFieldTuples.Add("FCONTACTUNITNAME", "max(IsNull(B2.FNAME,' ')) as FCONTACTUNITNAME");
            tableFieldTuples.Add("FCURRENCYFORNAME", "max(IsNull(T73.FNAME,' ')) AS FCURRENCYFORNAME");
            tableFieldTuples.Add("FBUSINESSORGID", "max(IsNull(T1.FBUSINESSORGID,0))");
            tableFieldTuples.Add("FBUSINESSORGNAME", "max(ISNULL(B4.FNAME,' ')) AS FBUSINESSORGNAME");
            tableFieldTuples.Add("FSETTLEORGNAME", "max(IsNull(B3.FNAME,' ')) AS FSETTLEORGNAME");
            tableFieldTuples.Add("FRPORGID", "max(IsNull(T1.FRPORGID,0))");
            tableFieldTuples.Add("FRPORGNAME", "max(IsNull(B33.FNAME,' ')) AS FRPORGNAME");
            if (flag)
            {
                tableFieldTuples.Add("FSEQ", "max(IsNull(T1.FSEQ,0))");
                if (this.FilterCondition.GroupbyFields.Contains("FMaterialNumber"))
                {
                    tableFieldTuples.Add("FMaterialID", "max(IsNull(T1.FMATERIALID,0))");
                    tableFieldTuples.Add("FMaterialNumber", "max(IsNull(T1.FMATERIALNUMBER,' ')) ");
                }
                else
                {
                    tableFieldTuples.Add("FMaterialID", "0");
                    tableFieldTuples.Add("FMaterialNumber", "' '");
                }
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSDEPTNAME"))
            {
                tableFieldTuples.Add("FBUSINESSDEPTID", "max(IsNull(T1.FBUSINESSDEPTID,0))");
                tableFieldTuples.Add("FBUSINESSDEPTNAME", "max(ISNULL(B5.FNAME,' ')) AS FBUSINESSDEPTNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSGROUPNAME"))
            {
                tableFieldTuples.Add("FBUSINESSGROUPID", "max(IsNull(T1.FBUSINESSGROUPID,0))");
                tableFieldTuples.Add("FBUSINESSGROUPNAME", "max(ISNULL(B7.FNAME,' ')) AS FBUSINESSGROUPNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBUSINESSERNAME"))
            {
                tableFieldTuples.Add("FBUSINESSERID", "max(IsNull(T1.FBUSINESSERID,0))");
                tableFieldTuples.Add("FBUSINESSERNAME", "max(ISNULL(B8.FNAME,' ')) AS FBUSINESSERNAME");
            }
            if (this.FilterCondition.GroupbyFields.Contains("FBILLTYPENAME"))
            {
                tableFieldTuples.Add("FBILLTYPEID", "max(IsNull(T1.FBILLTYPEID,0))");
                tableFieldTuples.Add("FBILLTYPENAME", "max(ISNULL(T69.FNAME,' ')) AS FBILLTYPENAME");
            }
            tableFieldTuples.Add("FBUSINESSDESP", "max(ISNULL(C1.FNAME,' ')) AS FBUSINESSDESP");
            tableFieldTuples.Add("FCURRENCYNAME", "max(ISNULL(T72.FNAME,' ')) AS FCURRENCYNAME");
            tableFieldTuples.Add("FLEFTAMOUNTFOR", string.Format("sum(case T1.FBUSINESSTYPEID when {0} then T1.FLEFTAMOUNTFOR else 0 end)", Convert.ToInt32(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance)));
            tableFieldTuples.Add("FLEFTAMOUNT", string.Format("sum(case T1.FBUSINESSTYPEID when {0} then T1.FLEFTAMOUNT else 0 end)", Convert.ToInt32(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance)));
            tableFieldTuples.Add("FAMOUNTDIGITSFOR", "max(T70.FAMOUNTDIGITS) AS FAMOUNTDIGITSFOR");
            tableFieldTuples.Add("FAMOUNTDIGITS", "max(ISNULL(T71.FAMOUNTDIGITS,2)) AS FAMOUNTDIGITS");
            tableFieldTuples.Add("FQTYDIGITS", "max(ISNULL(T1.FQTYDIGITS,10)) AS FQTYDIGITS");
            tableFieldTuples.Add("FPRICEDIGITS", "max(ISNULL(T70.FPRICEDIGITS,10)) AS FPRICEDIGITS");
            tableFieldTuples.Add("FDetailType", string.Format("{0} AS FDetailType", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BeginBalance)));
            stringBuilder.AppendFormat("Insert Into {0}({1})", this.tmpTable_RptData_Sum, tableFieldTuples.Table1Fields);
            stringBuilder.AppendFormat(" SELECT {0}", tableFieldTuples.Table2Fields);
            stringBuilder.AppendFormat(" From {0} T1 ", this.tmpTable_RptData_Detail);
            stringBuilder.AppendLine(this.FilterString_LeftJoinSumRptData(AbstractDetailReportService.RowDataOrder.BeginBalance));
            stringBuilder.AppendFormat(" Where 1=1 {0} {1}", (!string.IsNullOrWhiteSpace(this.FilterCondition.AdvanceFilterString)) ? " And " : " ", this.ConvertFilterFieldName(this.FilterCondition.AdvanceFilterString));
            if (this.SubSystemID == "AP")
            {
                stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "BD_Supplier", "FCONTACTUNITID", "T1"));
            }
            else
            {
                stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "BD_Customer", "FCONTACTUNITID", "T1"));
            }
            stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "ORG_Organizations", "FBUSINESSORGID", "T1"));
            if (!string.IsNullOrWhiteSpace(this.FilterCondition.FormId) && (this.FilterCondition.FormId == "AR_ContactDetail" || this.FilterCondition.FormId == "AP_ContactDetail"))
            {
                stringBuilder.AppendFormat("   Group by {0} ", this.ConvertFilterFieldName(this.FilterCondition.GroupbyFields));
            }
            else
            {
                stringBuilder.AppendFormat("   Group by B21.FFormID,{0} ", this.ConvertFilterFieldName(this.FilterCondition.GroupbyFields));
            }
            list.Add(new SqlObject(stringBuilder.ToString(), list2));
            tableFieldTuples.Clear();
            stringBuilder.Clear();
            list2.Clear();
            tableFieldTuples.Add("FID", "T1.FID");
            tableFieldTuples.Add("FFORMID", "T1.FFORMID");
            tableFieldTuples.Add("FBILLNO", "T1.FBILLNO");
            tableFieldTuples.Add("FDATE", "max(T1.FDATE)");
            tableFieldTuples.Add("FBUSINESSRANK", "T1.FBUSINESSRANK");
            tableFieldTuples.Add("FCONTACTUNITTYPE", "max(IsNull(B21.FFormID,' '))");
            tableFieldTuples.Add("FCONTACTUNITID", "max(IsNull(B21.FMASTERID,0))");
            tableFieldTuples.Add("FCONTACTUNITNUMBER", "max(IsNull(B21.FNumber,' ')) as FCONTACTUNITNUMBER");
            tableFieldTuples.Add("FCONTACTUNITNAME", "max(IsNull(B2.FNAME,' ')) as FCONTACTUNITNAME");
            tableFieldTuples.Add("FREMARK", "max(IsNull(T1.FREMARK,' ')) as FREMARK");
            tableFieldTuples.Add("FORDER", "max(IsNull(T1.FORDER,' ')) as FORDER");
            tableFieldTuples.Add("FTRANSFER", "max(IsNull(T1.FTRANSFER,' ')) as FTRANSFER");
            tableFieldTuples.Add("FCURRENCYFORNAME", "max(IsNull(T73.FNAME,' ')) AS FCURRENCYFORNAME");
            tableFieldTuples.Add("FBUSINESSORGID", "max(IsNull(T1.FBUSINESSORGID,0))");
            tableFieldTuples.Add("FBUSINESSORGNAME", "max(ISNULL(B4.FNAME,' ')) AS FBUSINESSORGNAME");
            tableFieldTuples.Add("FSETTLEORGNAME", "max(IsNull(B3.FNAME,' ')) AS FSETTLEORGNAME");
            tableFieldTuples.Add("FRPORGID", "max(IsNull(T1.FRPORGID,0))");
            tableFieldTuples.Add("FRPORGNAME", "max(IsNull(B33.FNAME,' ')) AS FRPORGNAME");
            tableFieldTuples.Add("FBUSINESSDEPTID", "max(IsNull(T1.FBUSINESSDEPTID,0))");
            tableFieldTuples.Add("FBUSINESSDEPTNAME", "max(ISNULL(B5.FNAME,' ')) AS FBUSINESSDEPTNAME");
            tableFieldTuples.Add("FBUSINESSGROUPID", "max(IsNull(T1.FBUSINESSGROUPID,0))");
            tableFieldTuples.Add("FBUSINESSGROUPNAME", "max(ISNULL(B7.FNAME,' ')) AS FBUSINESSGROUPNAME");
            tableFieldTuples.Add("FBUSINESSERID", "max(IsNull(T1.FBUSINESSERID,0))");
            tableFieldTuples.Add("FBUSINESSERNAME", "max(ISNULL(B8.FNAME,' ')) AS FBUSINESSERNAME");
            tableFieldTuples.Add("FBUSINESSDESP", "max(ISNULL(C1.FNAME,' ')) AS FBUSINESSDESP");
            tableFieldTuples.Add("FBILLTYPEID", "max(ISNULL(T1.FBILLTYPEID,' ')) As FBILLTYPEID");
            tableFieldTuples.Add("FBILLTYPENAME", "max(ISNULL(T69.FNAME,' ')) As FBILLTYPENAME");
            tableFieldTuples.Add("FCURRENCYNAME", "max(ISNULL(T72.FNAME,' ')) AS FCURRENCYNAME");
            tableFieldTuples.Add("FCURRENCYRECNAME", "max(IsNull(T75.FNAME,' ')) AS FCURRENCYRECNAME");
            tableFieldTuples.Add("FREFUNDAMOUNTFOR", "sum(T1.FREFUNDAMOUNTFOR)");
            if (this.SubSystemID == "ARAP")
            {
                if (flag)
                {
                    tableFieldTuples.Add("FSEQ", "T1.FSEQ");
                    tableFieldTuples.Add("FENTRYID", "max(isnull(T1.FENTRYID,0))");
                    tableFieldTuples.Add("FMaterialID", "T1.FMaterialID");
                    tableFieldTuples.Add("FOLEFTAMOUNT", "T1.FOLEFTAMOUNT");
                    tableFieldTuples.Add("FUnitName", "T1.FUnitName");
                    tableFieldTuples.Add("FQty", "sum(T1.FQty) as FQty");
                }
                tableFieldTuples.Add("FAMOUNT", "sum(T1.FAMOUNT) - sum(T1.FREALAMOUNT) - sum(T1.FOFFAMOUNT)");
                tableFieldTuples.Add("FAMOUNTFOR", "sum(T1.FAMOUNTFOR) - sum(T1.FREALAMOUNTFOR) - sum(T1.FOFFAMOUNTFOR)");
            }
            else
            {
                if (flag)
                {
                    tableFieldTuples.Add("FMaterialID", "T1.FMaterialID");
                    tableFieldTuples.Add("FMaterialNumber", "T1.FMaterialNumber");
                    tableFieldTuples.Add("FUnitName", "T1.FUnitName");
                    tableFieldTuples.Add("FQty", "sum(T1.FQty) as FQty");
                    tableFieldTuples.Add("FOLEFTAMOUNT", "T1.FOLEFTAMOUNT");
                }
                tableFieldTuples.Add("FAMOUNT", "sum(T1.FAMOUNT)");
                tableFieldTuples.Add("FHADIVAMOUNT", "sum(T1.FHADIVAMOUNT)");
                tableFieldTuples.Add("FREALAMOUNT", "sum(T1.FREALAMOUNT)");
                tableFieldTuples.Add("FOFFAMOUNT", "sum(T1.FOFFAMOUNT)");
                tableFieldTuples.Add("FAMOUNTFOR", "sum(T1.FAMOUNTFOR)");
                tableFieldTuples.Add("FHADIVAMOUNTFOR", "sum(T1.FHADIVAMOUNTFOR)");
                tableFieldTuples.Add("FREALAMOUNTFOR", "sum(T1.FREALAMOUNTFOR)");
                tableFieldTuples.Add("FOFFAMOUNTFOR", "sum(T1.FOFFAMOUNTFOR)");
            }
            tableFieldTuples.Add("FAMOUNTDIGITSFOR", "max(T70.FAMOUNTDIGITS) AS FAMOUNTDIGITSFOR");
            tableFieldTuples.Add("FAMOUNTDIGITS", "max(ISNULL(T71.FAMOUNTDIGITS,2)) AS FAMOUNTDIGITS");
            tableFieldTuples.Add("FQTYDIGITS", "max(ISNULL(T1.FQTYDIGITS,10)) AS FQTYDIGITS");
            tableFieldTuples.Add("FPRICEDIGITS", "max(ISNULL(T70.FPRICEDIGITS,10)) AS FPRICEDIGITS");
            tableFieldTuples.Add("FDetailType", string.Format("{0} AS FDetailType", Convert.ToInt32(AbstractDetailReportService.RowDataOrder.BillDetail)));
            stringBuilder.AppendFormat("Insert Into {0}({1})", this.tmpTable_RptData_Sum, tableFieldTuples.Table1Fields);
            stringBuilder.AppendFormat(" SELECT {0}", tableFieldTuples.Table2Fields);
            stringBuilder.AppendFormat(" From {0} T1 ", this.tmpTable_RptData_Detail);
            stringBuilder.AppendLine(this.FilterString_LeftJoinSumRptData(AbstractDetailReportService.RowDataOrder.BillDetail));
            stringBuilder.AppendFormat(" Where 1=1 {0} {1}", (!string.IsNullOrWhiteSpace(this.FilterCondition.AdvanceFilterString)) ? " And " : " ", this.ConvertFilterFieldName(this.FilterCondition.AdvanceFilterString));
            stringBuilder.AppendFormat(" And T1.FBUSINESSTYPEID <> {0}", Convert.ToInt32(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance));
            if (this.SubSystemID == "AP")
            {
                stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "BD_Supplier", "FCONTACTUNITID", "T1"));
            }
            else
            {
                stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "BD_Customer", "FCONTACTUNITID", "T1"));
            }
            stringBuilder.AppendLine(this.FilterDataPermission(this.lstDataTempTable, "ORG_Organizations", "FBUSINESSORGID", "T1"));
            stringBuilder.AppendFormat(" GROUP BY {0},T1.FID,T1.FFORMID,T1.FBILLNO,T1.FBUSINESSRANK,B21.FFormID,T1.FSEQ", this.ConvertFilterFieldName(this.FilterCondition.GroupbyFields));
            if (this.SubSystemID == "ARAP")
            {
                if (flag)
                {
                    stringBuilder.AppendLine(",T1.FMaterialID,T1.FUnitName,T1.FOLEFTAMOUNT");
                }
            }
            else if (flag)
            {
                stringBuilder.AppendLine(",T1.FMaterialID,T1.FMaterialNumber,T1.FUnitName,T1.FOLEFTAMOUNT");
            }
            list.Add(new SqlObject(stringBuilder.ToString(), list2));
            return list;
        }

        /*处理左连接***/
        private string FilterString_LeftJoinSumRptData(AbstractDetailReportService.RowDataOrder rowType)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string arg = "T1.FBUSINESSTYPEID";
            if (rowType == AbstractDetailReportService.RowDataOrder.BeginBalance)
            {
                arg = Convert.ToInt32(AbstractDetailReportService.BusinessTYPEENUM.BeginBalance).ToString();
            }
            if (rowType == AbstractDetailReportService.RowDataOrder.EndBalance)
            {
                arg = "8";
            }
            stringBuilder.AppendFormat(" LEFT JOIN V_FIN_CONTACTTYPE B21 ON T1.FCONTACTUNITID = B21.FITEMID", new object[0]);
            stringBuilder.AppendFormat(" LEFT JOIN V_FIN_CONTACTTYPE_L B2 ON B21.FITEMID = B2.FITEMID AND B2.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_CUSTOMER BC1 ON B21.FITEMID=BC1.FCUSTID", new object[0]);
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_CUSTOMER_L BC1L ON BC1L.FCUSTID=BC1.FCUSTID AND BC1L.FLOCALEID={0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_BAS_ASSISTANTDATAENTRY BA ON BA.FENTRYID=BC1.FCUSTTYPEID", new object[0]);
            stringBuilder.AppendFormat(" LEFT JOIN T_BAS_ASSISTANTDATAENTRY_L BAL ON BA.FENTRYID=BAL.FENTRYID AND BAL.FLOCALEID={0}", this.LCID);
            stringBuilder.AppendLine("   LEFT JOIN T_BD_CURRENCY T70 ON T70.FCURRENCYID = T1.FCURRENCYFORID ");
            stringBuilder.AppendLine("   LEFT JOIN T_BD_CURRENCY T71 ON T71.FCURRENCYID = T1.FCURRENCYID ");
            stringBuilder.AppendLine("   LEFT JOIN T_BD_CURRENCY T74 ON T74.FCURRENCYID = T1.FSETTLECURRENCYID ");
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_CURRENCY_L T72 ON T72.FCURRENCYID = T71.FCURRENCYID AND T72.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_CURRENCY_L T73 ON T73.FCURRENCYID = T70.FCURRENCYID AND T73.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_CURRENCY_L T75 ON T75.FCURRENCYID = T74.FCURRENCYID AND T75.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_ORG_ORGANIZATIONS_L B4 ON B4.FORGID = T1.FBUSINESSORGID AND B4.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_ORG_ORGANIZATIONS_L B3 ON B3.FORGID = T1.FSETTLEORGID AND B3.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_ORG_ORGANIZATIONS_L B33 ON B33.FORGID = T1.FRPORGID AND B33.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN T_BD_DEPARTMENT_L B5 ON B5.FDEPTID = T1.FBUSINESSDEPTID AND B5.FLOCALEID = {0}", this.LCID);
            stringBuilder.AppendFormat(" LEFT JOIN V_BD_OPERATORGROUP_L B7 ON B7.FENTRYID = T1.FBUSINESSGROUPID AND B7.FLOCALEID = {0}", this.LCID);
            string arg2 = "V_BD_OperatorStaff_L";
            if (this.SubSystemID == "AR")
            {
                arg2 = "V_BD_SALESMAN_L";
            }
            if (this.SubSystemID == "AP")
            {
                arg2 = "V_BD_BUYER_L";
            }
            stringBuilder.AppendFormat(" LEFT JOIN {0} B8 ON B8.FID = T1.FBUSINESSERID AND B8.FLOCALEID = {1}", arg2, this.LCID);
            string subSystemID;
            if ((subSystemID = this.SubSystemID) != null)
            {
                if (!(subSystemID == "AR"))
                {
                    if (!(subSystemID == "AP"))
                    {
                        if (subSystemID == "ARAP")
                        {
                            stringBuilder.AppendFormat(" LEFT JOIN V_ARAP_BusinessTYPEENUM_L C1 ON C1.FID = {0} AND C1.FLOCALEID = {1}", arg, this.LCID);
                        }
                    }
                    else
                    {
                        stringBuilder.AppendFormat(" LEFT JOIN T_AP_BusinessTYPEENUM_L C1 ON C1.FID = {0} AND C1.FLOCALEID = {1}", arg, this.LCID);
                    }
                }
                else
                {
                    stringBuilder.AppendFormat(" LEFT JOIN T_AR_BusinessTYPEENUM_L C1 ON C1.FID = {0} AND C1.FLOCALEID = {1}", arg, this.LCID);
                }
            }
            stringBuilder.AppendLine("   LEFT JOIN T_BAS_BillType B69 ON B69.FBILLTYPEID = T1.FBILLTYPEID ");
            stringBuilder.AppendFormat(" LEFT JOIN T_BAS_BillType_L T69 ON T69.FBILLTYPEID = B69.FBILLTYPEID AND T69.FLOCALEID = {0}", this.LCID);
            return stringBuilder.ToString();
        }
        /**原来的版本没有添加该方法 导致结果不对**/
        public override  List<SummaryField> GetSummaryColumnInfo(IRptParams filter)
        {
            return new List<SummaryField>
            {
                new SummaryField("FAMOUNTFOR", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FHadIVAmountFOR", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FREALAMOUNTFOR", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FOFFAMOUNTFOR", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FHADIVAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FREALAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FOFFAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new SummaryField("FAMOUNTDIGITSFOR", BOSEnums.Enu_SummaryType.MAX),
                new SummaryField("FAMOUNTDIGITS", BOSEnums.Enu_SummaryType.MAX),
                new SummaryField("FREFUNDAMOUNTFOR", BOSEnums.Enu_SummaryType.SUM),
                //new SummaryField("FQTYDIGITS", BOSEnums.Enu_SummaryType.SUM)
            };
        }
        protected override List<DecimalControlField> SetAmountDigit()
        {
            return new List<DecimalControlField>
            {
                new DecimalControlField("FAMOUNTDIGITSFOR", "FINITAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITSFOR", "FAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITSFOR", "FREALAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITSFOR", "FOFFAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITSFOR", "FLEFTAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITSFOR", "FHADIVAMOUNTFOR"),
                new DecimalControlField("FAMOUNTDIGITS", "FINITAMOUNT"),
                new DecimalControlField("FAMOUNTDIGITS", "FAMOUNT"),
                new DecimalControlField("FAMOUNTDIGITS", "FREALAMOUNT"),
                new DecimalControlField("FAMOUNTDIGITS", "FOFFAMOUNT"),
                new DecimalControlField("FAMOUNTDIGITS", "FLEFTAMOUNT"),
                new DecimalControlField("FAMOUNTDIGITS", "FHADIVAMOUNT")
            };
        }
        private string SetOrderByString(string GroupByString, string SortString)
        {
            if (string.IsNullOrWhiteSpace(SortString))
            {
                SortString = "FCONTACTUNITNUMBER,FCURRENCYFORNAME,FDetailType,FDATE,FBUSINESSRANK,FBILLNO";
            }
            List<string> first = SortString.Split(new char[]
            {
                ','
            }).ToList<string>();
            List<string> collection = GroupByString.Split(new char[]
            {
                ','
            }).ToList<string>();
            List<string> list = new List<string>();
            list.AddRange(collection);
            list.Add("FDetailType");
            list.Add("FDATE");
            list.Add("FBUSINESSRANK");
            list.Add("FBILLNO");
            list.AddRange(first.Except(list));
            return string.Join(",", list);
        }
        private List<long> GetLinkSupOrVend()
        {
            string text = "";
            List<long> list = new List<long>();
            if (this.SubSystemID == "ARAP")
            {
                string contactUnitType;
                if ((contactUnitType = this.FilterCondition.ContactUnitType) != null)
                {
                    if (!(contactUnitType == "BD_Supplier"))
                    {
                        if (contactUnitType == "BD_Customer")
                        {
                            text = " select distinct FSUPPLIERID as FContactID from T_BD_CUSTOMER    \r\n                                    where FSUPPLIERID<>0  ";
                            if (!ObjectUtils.IsNullOrEmptyOrWhiteSpace(this.FilterCondition.StartContactObj.Number))
                            {
                                text += string.Format(" and FNUMBER >='{0}' ", this.FilterCondition.StartContactObj.Number);
                            }
                            if (!ObjectUtils.IsNullOrEmptyOrWhiteSpace(this.FilterCondition.EndContactObj.Number))
                            {
                                text += string.Format(" and FNUMBER <='{0}' ", this.FilterCondition.EndContactObj.Number);
                            }
                        }
                    }
                    else
                    {
                        text = " select distinct t1.FCUSTOMERID as FContactID from T_BD_SUPPLIERFINANCE t1 \r\n                                                 inner join t_BD_Supplier t2 on t1.FSUPPLIERID =t2.FSUPPLIERID  \r\n                                                 where t1.FCUSTOMERID<>0 ";
                        if (!ObjectUtils.IsNullOrEmptyOrWhiteSpace(this.FilterCondition.StartContactObj.Number))
                        {
                            text += string.Format("  and t2.FNUMBER >='{0}' ", this.FilterCondition.StartContactObj.Number);
                        }
                        if (!ObjectUtils.IsNullOrEmptyOrWhiteSpace(this.FilterCondition.EndContactObj.Number))
                        {
                            text += string.Format(" and t2.FNUMBER <='{0}' ", this.FilterCondition.EndContactObj.Number);
                        }
                    }
                }
                if (!ObjectUtils.IsNullOrEmptyOrWhiteSpace(text))
                {
                    using (IDataReader dataReader = DBUtils.ExecuteReader(base.Context, text))
                    {
                        while (dataReader.Read())
                        {
                            list.Add(DBReaderUtils.GetValue<long>(dataReader, "FContactID"));
                        }
                        dataReader.Close();
                    }
                }
            }
            return list;
        }

        protected override void CreateTempTableAndIndex()
        {
            this.tmpTable_OrgEndDate = DBUtils.CreateSessionTemplateTable(base.Context, "TM_AR_ORGENDDATE", this.CreateTempTable_OrgEndDate());
            DBUtils.CreateSessionTemplateTableIndex(base.Context, string.Format("Create Index {0} on {1}({2})", "IDX_TM_AR_ORGENDDATE", this.tmpTable_OrgEndDate, "FSETTLEORGID"));
            this.tmpTable_RptData_Sum = DBUtils.CreateSessionTemplateTable(base.Context, "TM_AR_DETAILRPT_SUM", this.CreateTempTable_RptDataSum());
            DBUtils.CreateSessionTemplateTableIndex(base.Context, string.Format("Create Index {0} on {1}({2})", "IDX_TM_AR_DETAILRPT_SUM", this.tmpTable_RptData_Sum, "FID"));
            this.tmpTable_RptData_Detail = DBUtils.CreateSessionTemplateTable(base.Context, "TM_AR_DETAILRPT_DTL", this.CreateTempTable_RptDataDetail());
            DBUtils.CreateSessionTemplateTableIndex(base.Context, string.Format("Create Index {0} on {1}({2})", "IDX_TM_AR_DETAILRPT_DTL", this.tmpTable_RptData_Detail, "FCONTACTUNITID,FCURRENCYFORID"));
        }
        private string CreateTempTable_OrgEndDate()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(" (");
            stringBuilder.AppendLine(" FSETTLEORGID         int                  not null default 0,");
            stringBuilder.AppendLine(" FENDDATE             datetime             null,");
            stringBuilder.AppendLine(" FSysStartDate        datetime             null");
            stringBuilder.AppendLine(" )");
            return stringBuilder.ToString();
        }
        protected override void UpdateRowDataEndBalance(string bosTableName)
        {
        }
    }
}
