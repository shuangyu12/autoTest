import time
import json
import yaml
import random
import datetime
from copy import deepcopy
from typing import Callable, Dict, Any, List, Tuple

import sys
sys.path.append("E:\\customTest")
import pandas as pd
import aiohttp
import asyncio
from classification.classificationParallel import apiRequestAsync, getSessionAsync
from answerTest.answerTest import jsonAnswerAsync

class retryClass():

    @classmethod
    async def decorator(self, retryFunc, apiParams={}, isApiRequest=True, retryNum=3, *args, **kwargs):
        async for _ in range(retryNum):
            if isApiRequest:
                result = await self.apiRequestAsync(retryFunc, apiParams, kwargs)
            else:
                result = await retryFunc(self.args)
            if result[0]:
                break
        return result

    async def apiRequestAsync(fun: Callable[[aiohttp.ClientResponse, Any], Any], apiParams: dict, *args, **kwargs) -> tuple[bool, Any]:
        '''
        Docstring for kwargs param of apiRequestAsync 
        '''
        reqSucess = True
        if kwargs.get("timeStatc", None): 
            timeParams = {
                        "firstResponseTime": 0.0,
                        "totalResponseTime": 0.0,
                        "startTime":time.perf_counter()
                        }
            kwargs.update(timeParams)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(**apiParams) as response:
                    print("响应请求码：", response.status)
                    if response.status != 200:
                        raise Exception("请求状态码错误")
                    result = await fun(response, *args, **kwargs)
                    return reqSucess, result
        except Exception as e:
            reqSucess = False
            return reqSucess, str(e)


class getData():
    
    def __init__(self, *args, **kwargs):
        super(getData, self).__init__(*args, **kwargs)

    async def getReportInfo(response: aiohttp.ClientResponse, *args, **kwargs) -> List:
        result = await response.json()
        return result["data"]["rows"]

    async def getEsInfo(response: aiohttp.ClientResponse, *args, **kwargs) -> List:
        #通过API查询ES数据
        result = await response.json()
        result = result.get('data', {}).get('recordList', [])
        return result

    async def getResearchReportData(apiArgs,
                                    defaultResult: Dict = None,
                                    *args,
                                    **kwargs) -> Dict:
        if defaultResult is None:
            defaultResult = {
                "security_code": None, #公司代码
                "id": None, #传给大模型的id
                "fileName": "", #报告名称
                "filePath": "", #报告路径
                "fileDataId": "", #对应es里面的group
                "report_date": "", #报告期
                "stockName": "" #股票名称
            }
        
        researchReportData = {}
        researchReportInfo = await apiRequestAsync(getReportInfo, apiParams=apiArgs.get("getResearchReport"))

        if researchReportInfo[0]:
            esIndex = 'knowledge_base_content_report'
            stock_suffixes = [".SH", ".SZ", ".BJ"]
            report_types = [
                "季报点评", "中报点评", "年报点评", "公司深度研究报告"
            ]
            for data in researchReportInfo[1]:
                #查看es是否是点评报告
                if len(data.get("fileDataId", "")) == 0:
                    continue
                esQueryDsl = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"group": data.get("fileDataId")}}
                            ]
                        }
                    },
                    "size": kwargs.get("size", 1)
                }
                body = {
                    "indexName": esIndex,
                    "queryDsl": json.dumps(esQueryDsl), #使用java的接口
                }
                apiArgs.get("getEsInfo")["json"] = body
                esDataList = await apiRequestAsync(getEsInfo, apiParams=apiArgs.get("getEsInfo")) #返回一篇研报的一个切片
                if esDataList[0]:
                    meta = esDataList[1][0].get('source_data', {}).get('metadata', {})
                    #验证是否为点评类报告
                    subType = meta.get("report_sub_type", "")
                    if not any(report_type == subType for report_type in report_types):
                        continue

                    #验证是否为A股代码
                    security_code = meta.get("security_codes", "")
                    if isinstance(security_code, List):
                        security_code = security_code[0]
                    if not any(suffix in security_code for suffix in stock_suffixes):
                        continue

                    #时间是否为两年内
                    report_date_str = meta.get('archive_time', meta.get('archiveTime', ''))
                    end_date = datetime.datetime.now()
                    start_date = end_date - datetime.timedelta(days=2*365)
                    report_date = datetime.datetime.strptime(report_date_str, "%Y-%m-%d")
                    if not (start_date <= report_date <= end_date):
                        continue

                    result = deepcopy(defaultResult)
                    result["security_code"] = security_code
                    result["id"] = data.get("id")
                    result["fileName"] = data.get("fileName")
                    result["filePath"] = data.get("filePath")
                    result["fileDataId"] = data.get("fileDataId")
                    result["report_date"] = report_date_str
                    result["stockName"] = meta.get("stock_names", "")[0] if isinstance(meta.get("stock_names", ""), List) else meta.get("stock_names", "")
                    result["baseId"] = 1

                    if security_code not in researchReportData:
                        researchReportData[security_code] = [result]
                    else:
                        researchReportData[security_code].append(result)
        
        return researchReportData


async def getFinancialReportData(apiArgs,
                                defaultResult: Dict = None,
                                *args,
                                **kwargs) -> Dict:

    if defaultResult is None:
        defaultResult = {
            "security_code": None, #公司代码
            "id": None, #传给大模型的id
            "fileName": "", #报告名称
            "filePath": "", #报告路径
            "fileDataId": "", #对应es里面的group
            "report_date": "", #报告期
            "stockName": "" #股票名称
        }
    
    financialReportData = {}
    financialReportInfo = await apiRequestAsync(getReportInfo, apiParams=apiArgs.get("getFinancialReport"))

    if financialReportInfo[0]:
        esIndex = 'knowledge_base_content_financial_report'
        stock_suffixes = [".SH", ".SZ", ".BJ"]
        for data in financialReportInfo[1]:
            if len(data.get("fileDataId", "")) == 0:
                continue

            esQueryDsl = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"group": data.get("fileDataId")}}
                        ]
                    }
                },
                "size": kwargs.get("size", 1)
            }
            body = {
                "indexName": esIndex,
                "queryDsl": json.dumps(esQueryDsl), #使用java的接口
            }
            apiArgs.get("getEsInfo")["json"] = body
            esDataList = await apiRequestAsync(getEsInfo, apiParams=apiArgs.get("getEsInfo")) #返回一篇研报的一个切片
            if esDataList[0]:
                if len(esDataList[1]) == 0:
                    continue #排除掉es里面查不到数据的
                meta = esDataList[1][0].get('source_data', {}).get('metadata', {})

                #验证是否为A股代码
                security_code = meta.get("security_codes", "")[0] if isinstance(meta.get("security_codes", ""), List) else meta.get("security_codes", "")
                if not security_code:
                    continue #股票代码为空直接去除
                if not any(suffix in security_code for suffix in stock_suffixes):
                    continue

                #时间是否为两年内且财报要为半年报以及年报
                report_date_str = meta.get('report_date', '')
                if not (report_date_str.endswith('-06-30') or report_date_str.endswith('-12-31')):
                    continue
                end_date = datetime.datetime.now()
                start_date = end_date - datetime.timedelta(days=2*365)
                report_date = datetime.datetime.strptime(report_date_str, "%Y-%m-%d")
                if not (start_date <= report_date <= end_date):
                    continue

                result = deepcopy(defaultResult)
                result["security_code"] = security_code
                result["id"] = data.get("id")
                result["fileName"] = data.get("fileName")
                result["filePath"] = data.get("filePath")
                result["fileDataId"] = data.get("fileDataId")
                result["report_date"] = report_date_str
                result["stockName"] = meta.get("stock_names", "")[0] if isinstance(meta.get("stock_names", ""), List) else meta.get("stock_names", "")
                result["baseId"] = 4

                if security_code not in financialReportData:
                    financialReportData[security_code] = [result]
                else:
                    financialReportData[security_code].append(result)
        
    return financialReportData

async def getFinallyData(
            apiFile:str = "./individualStockReview/apiInfo.yaml",
            defaultResult:dict = None,
            *args,
            **kwargs) -> List:  
    
    if defaultResult is None:
        defaultResult = {
            "security_code": None, #公司代码
            "stockName": "", #股票名称
            "firstPortId": None, #传给大模型的id
            "secondPortId": None,
            "firstFilePath": None, #报告路径
            "secondFilePath": None
        }
    
    with open(apiFile, "r", encoding="utf-8") as f:
        apiArgs = yaml.safe_load(f)

    researchReportData = await getResearchReportData(apiArgs)
    financialReportData = await getFinancialReportData(apiArgs)

    #整合分割数据
    finallyData = []
    totalData = {}
    for security_code, data in financialReportData.items():
        if security_code not in totalData:
            totalData[security_code] = {"financialReport":[], "researchReport":[]}
        data.sort(key = lambda x: datetime.datetime.strptime(x["report_date"], "%Y-%m-%d"))
        totalData[security_code]["financialReport"].extend(data)
        

    for security_code, data in researchReportData.items():
        if security_code not in totalData:
            totalData[security_code] = {"financialReport":[], "researchReport":[]}
        data.sort(key = lambda x: datetime.datetime.strptime(x["report_date"], "%Y-%m-%d"))
        totalData[security_code]["researchReport"].extend(data)
        
    #去除一下只有财报没有研报的
    for security_code, data in totalData.items():
        if len(data["researchReport"]) == 0:
            continue
        result = deepcopy(defaultResult)
        result["security_code"] = security_code
        result["stockName"] = data["researchReport"][0]["stockName"] if len(data["researchReport"]) != 0 else data["financialReport"][0]["stockName"]
        if len(data["researchReport"]) > 1 and len(data["financialReport"]) > 1: #研报与财报均要大于一篇，才会触发更新
            result["firstPortId"] = [(subdata["id"], subdata["baseId"], subdata["fileName"], subdata["report_date"]) for subdata in data["researchReport"][:-1] + data["financialReport"][:-1]]
            result["firstFilePath"] = [(subdata["id"], subdata["filePath"]) for subdata in data["researchReport"][:-1] + data["financialReport"][:-1]]
            result["secondPortId"] = [(subdata["id"], subdata["baseId"], subdata["fileName"], subdata["report_date"]) for subdata in [data["researchReport"][-1]] + [data["financialReport"][-1]]]
            result["secondFilePath"] = [(subdata["id"], subdata["filePath"]) for subdata in [data["researchReport"][-1]] + [data["financialReport"][-1]]]
        else:
            result["firstPortId"] = [(subdata["id"], subdata["baseId"], subdata["fileName"], subdata["report_date"]) for subdata in data["researchReport"] + data["financialReport"]]
            result["firstFilePath"] = [(subdata["id"], subdata["filePath"]) for subdata in data["researchReport"] + data["financialReport"]]
        finallyData.append(result)
    
    if kwargs.get("is_save", None):
        finallyData = pd.DataFrame(finallyData)
        finallyData.to_excel(kwargs.get("save_path", "./save_data_path.xlsx"))

    return finallyData

async def firstResult(
    apiFile:str = "./individualStockReview/apiInfo.yaml",
    finallyDataPath:str = "./save_firstResult_path.xlsx", #更新数据
    *args,
    **kwargs) -> List: 

    '''
    初稿生成
    '''

    finallyData = pd.read_excel(finallyDataPath, usecols=lambda col: not str(col).startswith("Unnamed"))
    finallyData = [finallyData.iloc[idx, :].to_dict() for idx in range(len(finallyData))]

    with open(apiFile, "r", encoding="utf-8") as f:
        apiArgs = yaml.safe_load(f)
    sessionParam = apiArgs.get("gfGetSession")
    agentChatParam = apiArgs.get("gfChatApi_1")
    sessionParam["json"].update({
        "agentId": agentChatParam.get("json").get("userId"),
        "agentBatchId": agentChatParam.get("json").get("botId")
    })
    for data in finallyData: 
        if not data.get("fisrtResult", None):
            data["fisrtResult"] = "error"
        #if data["fisrtResult"] != "error":
        #    continue
        if len(data["firstPortId"])==0:
            continue

        sessionId = await apiRequestAsync(getSessionAsync, sessionParam)
        recordDocs = []
        for idList in eval(data["firstPortId"]):
            recordDocs.append({
			"docName": idList[2] or "研报标题",
			"docId": idList[0] # id
		    })

        if sessionId[0]:
            sessionId = sessionId[1]
            agentChatParam["json"].update({
                "messages": "生成{}的框架".format(data.get("stockName")),
                "conversationId": sessionId,
                "recordDocs": recordDocs
            })
            chatResult = await apiRequestAsync(jsonAnswerAsync, agentChatParam, replaceTrace=True) #返回是一个json数组
            if not chatResult[0]:
                print(f"chat_1请求报错, 原因是{chatResult[1]}")
                data["fisrtResult"] = "error"
            else:
                data["fisrtResult"] = chatResult[1]
        else:
            print(f"chat_1获得获取session时报错, 原因是{sessionId[1]}")
    
    if kwargs.get("is_save", False):
        finallyData = pd.DataFrame(finallyData)
        finallyData.to_excel(kwargs.get("save_path", "./save_firstResult_path.xlsx"))
    
    return finallyData

async def update(data, sessionParam, agentChatParam, reportType:List = [1]):
    #分研报与财报更新
    sessionId = await apiRequestAsync(getSessionAsync, sessionParam)
    recordDocs = []
    for idList in eval(data["secondPortId"]):
        #
        if idList[1] not in reportType:
            continue
        recordDocs.append({
        "docName": idList[2] or "研报标题",
        "docId": idList[0] # id
        })
    
    #修复时error使用
    #if not data["updateResult_{}".format("_".join([str(_) for _ in reportType]))] == "error":
    #    return data

    if len(recordDocs) == 0:
        data["updateResult_{}".format("_".join([str(_) for _ in reportType]))] = None
        return data
    
    if sessionId[0]:
        sessionId = sessionId[1]
        agentChatParam["json"].update({
            "messages": "更新{}的框架".format(data.get("stockName")),
            "conversationId": sessionId,
            "recordDocs": recordDocs,
            "dynamicPrompt":{
                "framework": data.get("fisrtResult")
            }
        })
        
        chatResult = await apiRequestAsync(jsonAnswerAsync, agentChatParam, replaceTrace=True) #返回是一个json串
        if not chatResult[0]:
            print(f"chat_2请求报错, 原因是{chatResult[1]}")
            data["updateResult_{}".format("_".join([str(_) for _ in reportType]))] = "error"
        else:
            data["updateResult_{}".format("_".join([str(_) for _ in reportType]))] = chatResult[1]
    else:
        print(f"chat_2获得获取session时报错, 原因是{sessionId[1]}")
    
    return data

async def updateResult(
        apiFile:str = "./individualStockReview/apiInfo.yaml",
        finallyDataPath:str = "./save_firstResult_path.xlsx",
        *args,
        **kwargs) -> List:

    '''
    模板更新：分研报更新与财报更新
    '''  

    finallyData = pd.read_excel(finallyDataPath, usecols=lambda col: not str(col).startswith("Unnamed"))
    finallyData = [finallyData.iloc[idx, :].to_dict() for idx in range(len(finallyData))]

    with open(apiFile, "r", encoding="utf-8") as f:
        apiArgs = yaml.safe_load(f)
    sessionParam = apiArgs.get("gfGetSession")

    for data in finallyData: 
        '''
        没有初稿、或者是没有更新稿的直接跳过
        '''
        if pd.isnull(data["secondPortId"]) or data.get("fisrtResult", "error") == "error":
            continue

        agentChatParam = apiArgs.get("gfChatApi_2")
        sessionParam["json"].update({
            "agentId": agentChatParam.get("json").get("userId"),
            "agentBatchId": agentChatParam.get("json").get("botId")
        })
        data = await update(data, sessionParam, agentChatParam, [1])
        data = await update(data, sessionParam, agentChatParam, [4])
        data = await update(data, sessionParam, agentChatParam, [1, 4])

    if kwargs.get("is_save", False):
        finallyData = pd.DataFrame(finallyData)
        finallyData.to_excel(kwargs.get("save_path", "./save_updateResult_path.xlsx"))
    
    return finallyData

async def simpleResult(
        apiFile:str = "./individualStockReview/apiInfo.yaml",
        finallyDataPath:str = "./save_updateResult_path.xlsx",
        *args,
        **kwargs) -> List:
    '''
    简化框架,
    选择最新财报以及财报的更新模板进行更新。
    '''

    finallyData = pd.read_excel(finallyDataPath, usecols=lambda col: not str(col).startswith("Unnamed"))
    finallyData = [finallyData.iloc[idx, :].to_dict() for idx in range(len(finallyData))]

    with open(apiFile, "r", encoding="utf-8") as f:
        apiArgs = yaml.safe_load(f)
    sessionParam = apiArgs.get("gfGetSession")

    for data in finallyData: 
        '''
        需要去除的情况: 
        1、没有底稿的不更新。
        2、没有最新财报进行更新。
        3、选择最新财报以及研报的更新模板进行更新。
        '''
        if pd.isnull(data["simpleResult"]):
            data["simpleResult"] = None
        if data["simpleResult"] != "error":   #用于更新错误数据使用
            continue

        if pd.isnull(data["secondPortId"]):
            portId = eval(data["firstPortId"])
        else:
            portId = eval(data["firstPortId"]) + eval(data["secondPortId"])
        if not any([_[1] == 4 for _ in portId]):
            continue

        agentChatParam = apiArgs.get("gfChatApi_3")
        sessionParam["json"].update({
            "agentId": agentChatParam.get("json").get("userId"),
            "agentBatchId": agentChatParam.get("json").get("botId")
        })

        sessionId = await apiRequestAsync(getSessionAsync, sessionParam)
        recordDocs = []
        for idList in portId:
            if idList[1] != 4:
                continue
            recordDocs.append({
            "docName": idList[2] or "研报标题",
            "docId": idList[0] # id
            })
        
        if not pd.isnull(data.get("updateResult_1_4", None)):
            framework = data.get("updateResult_1_4")
        elif not pd.isnull(data.get("updateResult_4", None)):
            framework = data.get("updateResult_4")
        elif not pd.isnull(data.get("updateResult_1", None)):
            framework = data.get("updateResult_1")
        else:
            framework = data.get("firstResult")
        
        if sessionId[0]:
            sessionId = sessionId[1]
            agentChatParam["json"].update({
                "messages": "结合{}的新增素材，得到本次问答框架。".format(data.get("stockName")),
                "conversationId": sessionId,
                "recordDocs": [recordDocs[-1]],
                "dynamicPrompt":{
                    "framework": framework
                }
            })
            chatResult = await apiRequestAsync(jsonAnswerAsync, agentChatParam, replaceTrace=True) #返回是一个json串
            if not chatResult[0]:
                print(f"chat_3请求报错, 原因是{chatResult[1]}")
                data["simpleResult"] = "error"
            else:
                data["simpleResult"] = chatResult[1]
        else:
            print(f"chat_3获得获取session时报错, 原因是{sessionId[1]}")

    if kwargs.get("is_save", False):
        finallyData = pd.DataFrame(finallyData)
        finallyData.to_excel(kwargs.get("save_path", "./save_simpleResult_path.xlsx"))
    
    return finallyData

async def businessAnalysis(
        apiFile:str = "./individualStockReview/apiInfo.yaml",
        finallyDataPath:str = "./save_simpleResult_path.xlsx",
        *args,
        **kwargs) -> List:
    '''
    1、默认模板
    2、不同时间的财报
    3、如果选季报, 则需要找最新时间
    '''
    finallyData = pd.read_excel(finallyDataPath, usecols=lambda col: not str(col).startswith("Unnamed"))
    finallyData = [finallyData.iloc[idx, :].to_dict() for idx in range(len(finallyData))]

    with open(apiFile, "r", encoding="utf-8") as f:
        apiArgs = yaml.safe_load(f)
    sessionParam = apiArgs.get("gfGetSession")

    agentChatParam = apiArgs.get("gfChatApi_4")
    sessionParam["json"].update({
        "agentId": agentChatParam.get("json").get("userId"),
        "agentBatchId": agentChatParam.get("json").get("botId")
    })
    for data in finallyData: 
        '''
        需要去掉无业绩分析大纲
        '''
        if pd.isnull(data.get("simpleResult", None)):
            continue
        #if data.get("defaultResult", None) != "error":  #用于修复失败
        #    continue
        sessionId = await apiRequestAsync(getSessionAsync, sessionParam)
        recordDocs = [] 
        if pd.isnull(data["secondPortId"]):
            portId = eval(data["firstPortId"])
        else:
            portId = eval(data["firstPortId"]) + eval(data["secondPortId"])
        financialReportData = [_ for _ in portId if _[1] == 4]
        if len(financialReportData) == 0:
            continue
        for idList in financialReportData:
            recordDocs.append({
            "docName": idList[2] or "研报标题",
            "docId": idList[0] # id
            })
        
        if kwargs.get("useDefault", False):
            idx = random.choice(list(range(len(recordDocs)))) #这里不需要随机抽取，选择最新的即可，因为业务大纲是根据最新生成的。
        else:
            idx =  -1
        recordDocs = [recordDocs[idx]]
        report_date = financialReportData[idx][3]
        if report_date.endswith('-06-30'):
            report_type = "半年度"
        elif report_date.endswith('-12-31'):
            report_type = "年度"
        else:
            report_type = ""
        
        report_type = "{}年{}".format(report_date.split("-")[0], report_type)

        if sessionId[0]:
            sessionId = sessionId[1]
            agentChatParam["json"].update({
                "messages": "结合{}的新增素材, 撰写{}的{}的业务分析".format(data.get("stockName"), data.get("stockName"), report_type),
                "conversationId": sessionId,
                "recordDocs": recordDocs
            })
            if not kwargs.get("useDefault", False):
                agentChatParam["json"].update({
                    "dynamicPrompt":{
                        "company_frame": data.get("simpleResult")
                    }
                })
            chatResult = await apiRequestAsync(jsonAnswerAsync, agentChatParam, notTranJson=True, replaceTrace=True) #返回是一个json串
            
            if not kwargs.get("useDefault", False):
                data["chatResultData"] = financialReportData[idx]
                if not chatResult[0]:
                    print(f"chat_4请求报错, 原因是{chatResult[1]}")
                    data["chatResult"] = "error"
                else:
                    data["chatResult"] = chatResult[1]
            else:
                data["defaultResultData"] = financialReportData[idx]
                if not chatResult[0]:
                    print(f"chat_4请求报错, 原因是{chatResult[1]}")
                    data["defaultResult"] = "error"
                else:
                    data["defaultResult"] = chatResult[1]
        else:
            print(f"chat_4获得获取session时报错, 原因是{sessionId[1]}")

    if kwargs.get("is_save", False):
        finallyData = pd.DataFrame(finallyData)
        finallyData.to_excel(kwargs.get("save_path", "./save_chatResult_path.xlsx"))
    
    return finallyData

# 异步批量执行任务
async def mainAsync():

    semaphore = asyncio.Semaphore(1)  # 限制最大并发数为 10 （可以控制并发，实现单协程与多协程的测试）

    async def bounded_getResultAsync(**kwargs):
        async with semaphore:
            return await businessAnalysis(**kwargs)#通过改这个函数可以获取数据、请求模型 getFinallyData(**kwargs)
    

    _ = await bounded_getResultAsync(is_save=True, finallyDataPath="./save_chatResult_path.xlsx", useDefault=False) #结果会保存为excel
    
    print("异步执行完成，结果已保存。")

if __name__ == "__main__":
    asyncio.run(mainAsync())