using Renci.SshNet;
using SAP.Middleware.Connector;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DailyCardDataUpload
{
    class Program
    {
        static RfcDestination m_rfcDestination = null;
        static RfcRepository m_rfcRepository = null;
        static IRfcFunction m_rfcFunction;
		static string targetDate = string.Empty;
        static string targetDateStart = string.Empty;
        static string targetDateEnd = string.Empty;
		static DataTable dtCard = new DataTable();
		static DataTable dtKakao = new DataTable();
		static DataTable dtZero = new DataTable();

		static void Main(string[] args)
        {
			if (args.Length == 0) // 자동 (날짜 인수 없을 때)
			{
				targetDate = DateTime.Today.AddDays(-1).ToString("yyyyMMdd");
				FtpDownloadUsingWebClient();
				ParsingData();
				RFCInfoSet("[SD] KIS 카드입금정보 및 수수료 정보", "*");
				InsertRfcDataImport("I_ZECDAT", targetDate);
				string l_strTableName = "IT_SALES";
				InsertRfcDataRows("*", l_strTableName, dtCard);
				RfcFunctionCall();
				string l_strErrorCode = m_rfcFunction.GetValue("E_RESULT").ToString();
				string l_strErrorMessage = m_rfcFunction.GetValue("E_MESSAGE").ToString();
				WriteLog("RFC 결과 : " + l_strErrorCode + "\t" + l_strErrorMessage);
				DeleteFile();
			}
			else if (args.Length == 1) // 수동 (날짜 인수 1개 있을 때 : 특정 날짜만)
			{
				targetDate = args[0].ToString();
				FtpDownloadUsingWebClient();
				ParsingData();
				RFCInfoSet("[SD] KIS 카드입금정보 및 수수료 정보", "*");
				InsertRfcDataImport("I_ZECDAT", targetDate);
				string l_strTableName = "IT_SALES";
				InsertRfcDataRows("*", l_strTableName, dtCard);
				RfcFunctionCall();
				string l_strErrorCode = m_rfcFunction.GetValue("E_RESULT").ToString();
				string l_strErrorMessage = m_rfcFunction.GetValue("E_MESSAGE").ToString();
				WriteLog("RFC 결과 : " + l_strErrorCode + "\t" + l_strErrorMessage);
				DeleteFile();
			}
            else if (args.Length == 2) // 수동 (날짜 인수 2개 있을 때 : 특정 기간동안)
            {
                targetDateStart = args[0].ToString();
                targetDateEnd = args[1].ToString();
                targetDate = targetDateStart;
                int count = (DateTime.ParseExact(targetDateEnd, "yyyyMMdd", null) - DateTime.ParseExact(targetDateStart, "yyyyMMdd", null)).Days;
                for (int i = 0; i <= count; i++)
                {
                    FtpDownloadUsingWebClient();
                    ParsingData();
                    RFCInfoSet("[SD] KICC 카드입금정보 및 수수료 정보", "*");
                    InsertRfcDataImport("I_ZECDAT", targetDate);
                    string l_strTableName = "IT_SALES";
                    InsertRfcDataRows("*", l_strTableName, dtCard);
                    RfcFunctionCall();
                    string l_strErrorCode = m_rfcFunction.GetValue("E_RESULT").ToString();
                    string l_strErrorMessage = m_rfcFunction.GetValue("E_MESSAGE").ToString();
                    WriteLog("RFC 결과 : " + l_strErrorCode + "\t" + l_strErrorMessage);
                    DeleteFile();
                    dtCard = new DataTable(); // 작업 완료 후 초기화
                    targetDate = DateTime.ParseExact(targetDate, "yyyyMMdd", null).AddDays(1).ToString("yyyyMMdd");
                }
            }
        }

		public static void FtpDownloadUsingWebClient()
		{
			var ci = new ConnectionInfo("*.*.*.*", "ypbooks", new PasswordAuthenticationMethod("ypbooks", "*"));

			string cardFtpPath = @"./workspace/rep/YPBOOKS001_REP." + targetDate;
			string kakaoFtpPath = @"./workspace/rep/YPBOOKS001_KAKAO_REP." + targetDate;
			string zeroFtpPath = @"./workspace/zero/YPBOOKS001_ZPP." + targetDate;
			string cardOutputFile = "YPBOOKS001_REP." + targetDate;
			string kakaoOutputFile = "YPBOOKS001_KAKAO_REP." + targetDate;
			string zeroOutputFile = "YPBOOKS001_ZPP." + targetDate;

			try
			{
				using (var sftp = new SftpClient(ci))
				{
					// SFTP 서버 연결
					sftp.Connect();

					// SFTP 다운로드
					using (var outfile = File.Create(cardOutputFile))
					{
						sftp.DownloadFile(cardFtpPath, outfile);
						WriteLog("카드 다운로드 완료 : " + cardOutputFile);
					}
					using (var outfile = File.Create(kakaoOutputFile))
					{
						sftp.DownloadFile(kakaoFtpPath, outfile);
						WriteLog("카카오 다운로드 완료 : " + kakaoOutputFile);
					}
					using (var outfile = File.Create(zeroOutputFile))
					{
						sftp.DownloadFile(zeroFtpPath, outfile);
						WriteLog("제로페이 다운로드 완료 : " + zeroOutputFile);
					}

					sftp.Disconnect();
				}
			}
			catch (Exception ex)
			{
				WriteLog(ex.Message);
			}
		}

		public static void DeleteFile()
		{
			FileInfo fi = new FileInfo("YPBOOKS001_REP." + targetDate);
			if (fi.Exists)
			{
				fi.Delete();
				WriteLog("카드 삭제 완료");
			}
			fi = new FileInfo("YPBOOKS001_KAKAO_REP." + targetDate);
			if (fi.Exists)
			{
				fi.Delete();
				WriteLog("카카오 삭제 완료");
			}
			fi = new FileInfo("YPBOOKS001_ZPP." + targetDate);
			if (fi.Exists)
			{
				fi.Delete();
				WriteLog("제로페이 삭제 완료");
			}
		}

		public static void ParsingData()
		{
            try
            {
                // 카드 데이터
                using (StreamReader sr = new StreamReader("YPBOOKS001_REP." + targetDate, Encoding.Default))
                {
                    dtCard.Columns.Add(new DataColumn("DR01", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR02", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR03", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR04", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR05", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR06", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR07", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR08", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR09", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR10", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR11", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR12", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR13", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR14", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR15", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR16", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("DR17", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("HR01", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("HR02", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("HR03", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("HR04", typeof(string)));
                    dtCard.Columns.Add(new DataColumn("HR05", typeof(string)));
                    string[] dr = new string[22];
                    string tmp = string.Empty;

                    while (true)
                    {
                        tmp = sr.ReadLine();

                        // START RECORD
                        if (tmp.Substring(0, 2).Equals("51"))
                        {
                            continue;
                        }

                        // HEADER RECORD
                        if (tmp.Substring(0, 2).Equals("52"))
                        {
                            dr[17] = tmp.Substring(18, 10); // 가맹점 사업자등록번호
                            dr[18] = tmp.Substring(40, 6);  // 지급일자
                            dr[19] = tmp.Substring(46, 15); // 가맹점번호
                            dr[20] = tmp.Substring(79, 6);  // 청구일자
                            dr[21] = tmp.Substring(85, 4);  // 카드사 코드
                            continue;
                        }

                        // DATA RECORD
                        /*
                         * 60 : 매출정상		61 : 매출반송		62 : 매출보류
                         * 63 : 매출보류해제	64 : 취소반송		65 : 취소보류
                         * 66 : 취소보류해제	67 : 취소매출정상
                         */
                        if (tmp.Substring(0, 2).Equals("60") || tmp.Substring(0, 2).Equals("61") || tmp.Substring(0, 2).Equals("62") ||
                            tmp.Substring(0, 2).Equals("63") || tmp.Substring(0, 2).Equals("64") || tmp.Substring(0, 2).Equals("65") ||
                            tmp.Substring(0, 2).Equals("66") || tmp.Substring(0, 2).Equals("67"))
                        {
                            dr[0] = tmp.Substring(0, 2);    // RECORD 구분
                            dr[1] = tmp.Substring(2, 3);    // 통화코드
                            dr[2] = tmp.Substring(5, 1);    // 통화지수
                            dr[3] = tmp.Substring(6, 6);    // 매출(취소)일자
                            dr[4] = tmp.Substring(12, 6);   // 접수일자
                            dr[5] = tmp.Substring(18, 19);  // 카드번호
                            dr[6] = tmp.Substring(37, 2);   // 할부기간
                            dr[7] = tmp.Substring(39, 10);  // 신용판매금액
                            dr[8] = tmp.Substring(49, 4);   // 반송사유 코드(카드사 반송코드)
                            dr[9] = tmp.Substring(53, 2);   // 반송사유 코드(VAN사 관리 표준 반송코드)
                            dr[10] = tmp.Substring(55, 13); // 주민등록번호
                            dr[11] = tmp.Substring(68, 10); // 수수료금액

                            // Substring 한글 자릿수 문제 해결
                            int count = 0;
                            char[] values = tmp.Substring(78, 40).ToCharArray();
                            foreach (char c in values)
                            {
                                int value = Convert.ToInt32(c);
                                if (value >= 0x80)
                                {
                                    count++;
                                }
                            }

                            dr[12] = tmp.Substring(78, 20 - count);  // 반송사유
                            dr[13] = tmp.Substring(98 - count, 20);  // 거래고유번호
                            dr[14] = tmp.Substring(118 - count, 20); // 단말기번호
                            dr[15] = tmp.Substring(138 - count, 10); // 승인번호
                            dr[16] = tmp.Substring(148 - count, 2);  // FILLER

                            // DataRow 추가
                            dtCard.Rows.Add(dr);
                            continue;
                        }

                        // TOTAL RECORD
                        if (tmp.Substring(0, 2).Equals("53"))
                        {
                            continue;
                        }

                        // EDI-END RECORD
                        if (tmp.Substring(0, 2).Equals("54"))
                        {
                            break;
                        }
                    }
                }

                /*
                // 카카오 데이터
                using (StreamReader sr = new StreamReader("YPBOOKS001_KAKAO_REP." + targetDate, Encoding.Default))
                {

                }

                // 제로페이 데이터
                using (StreamReader sr = new StreamReader("YPBOOKS001_ZPP." + targetDate, Encoding.Default))
                {

                }
                */
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message);
                dtCard = new DataTable(); // 오류 발생시 초기화
            }
        }

		public static bool RFCInfoSet(string _rfcName, string _rfcFunction)
        {
            try
            {
                // RFC 정보 셋팅
                RfcConfigParameters rfc = new RfcConfigParameters();
                rfc[RfcConfigParameters.Name] = "*";
                rfc[RfcConfigParameters.PoolSize] = Convert.ToString(190);
                rfc[RfcConfigParameters.PeakConnectionsLimit] = Convert.ToString(200);
                rfc[RfcConfigParameters.MaxPoolWaitTime] = Convert.ToString(1);
                rfc[RfcConfigParameters.Client] = "*";
                rfc[RfcConfigParameters.AppServerHost] = "*.*.*.*";
				rfc[RfcConfigParameters.User] = "*";
				rfc[RfcConfigParameters.Password] = "*";
				rfc[RfcConfigParameters.Language] = "KO";
				rfc[RfcConfigParameters.SystemNumber] = "1";
                rfc[RfcConfigParameters.Trace] = "0";
                m_rfcDestination = RfcDestinationManager.GetDestination(rfc);
                m_rfcRepository = m_rfcDestination.Repository;
                m_rfcFunction = m_rfcRepository.CreateFunction(_rfcFunction);
                return true;
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message);
                return false;
            }
        }

		public static bool InsertRfcDataImport(string _PramName, string _ParamValue)
		{
			try
			{
				m_rfcFunction.SetValue(_PramName, _ParamValue);
				return true;
			}
			catch (Exception ex)
			{
				WriteLog(ex.Message);
				return false;
			}
		}

		public static bool RfcFunctionCall()
		{
			bool l_bResult = false;

			try
			{
				m_rfcFunction.Invoke(m_rfcDestination);
				l_bResult = true;
			}
			catch (Exception ex)
			{
				WriteLog(ex.Message);

				string l_strErrorMessage = string.Empty;
				l_strErrorMessage = ex.Message;
				l_bResult = false;
			}
			return l_bResult;
		}

		public static bool InsertRfcDataRows(string _strStructureName, string _strTableName, DataTable _dt)
		{
			try
			{
				RfcStructureMetadata metaData = m_rfcDestination.Repository.GetStructureMetadata(_strStructureName);

				IRfcTable table = m_rfcFunction.GetTable(_strTableName);

				for (int i = 0; i < _dt.Rows.Count; i++)
				{
					IRfcStructure structData = metaData.CreateStructure();

					for (int j = 0; j < _dt.Columns.Count; j++)
					{
						string l_strColumnName = _dt.Columns[j].ColumnName;

						structData.SetValue(l_strColumnName, _dt.Rows[i][l_strColumnName]);
					}
					table.Append(structData);
				}

				m_rfcFunction.SetValue(_strTableName, table);
			}
			catch (Exception ex)
			{
				WriteLog(ex.Message);
				return false;
			}

			return true;
		}
		
		public static void WriteLog(string _message)
		{
			string filePath = Directory.GetCurrentDirectory() + @"\Logs\" + DateTime.Today.ToString("yyyyMMdd") + ".log";
			string dirPath = Directory.GetCurrentDirectory() + @"\Logs";
			string temp;

			DirectoryInfo dirInfo = new DirectoryInfo(dirPath);
			FileInfo fileInfo = new FileInfo(filePath);

			try
			{
				if (dirInfo.Exists != true)
				{
					Directory.CreateDirectory(dirPath);
				}

				if (fileInfo.Exists != true)
				{
					using (StreamWriter sw = new StreamWriter(filePath))
					{
						temp = string.Format("[{0}] {1}", DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"), _message);
						sw.WriteLine(temp);
						sw.Close();
					}
				}
				else
				{
					using (StreamWriter sw = File.AppendText(filePath))
					{
						temp = string.Format("[{0}] {1}", DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"), _message);
						sw.WriteLine(temp);
						sw.Close();
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
			}
		}
	}
}
