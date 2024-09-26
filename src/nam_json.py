import pandas as pd
import secrets
import string
from omegaconf import OmegaConf
from numbers import Number

class Metric_MS():
    def __init__(self):
        pass

    def dataKPI(self, name, runSql, description=None, valueSuffix=None, metricType=None):
        objKPI = {}
        objKPI['id']="".join(name.split(" "))
        objKPI['name']=name
        objKPI['runSql']=runSql
        if valueSuffix:
            objKPI['valueSuffix']=valueSuffix
        if description:
            objKPI['description']=description
        if metricType:
            objKPI['metricType']=metricType
        return objKPI

    def filterDefault(self, runSql):
        filter_default = {}
        filter_default['id']=''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)) + "_default"
        filter_default['type'] = "FILTER"
        filter_default['runSql'] = runSql
        return filter_default

    def filterMapping(self, kpiID, filterId):
        filterValue = {'id':filterId}
        filmap = {'kpiId':kpiID, 'filterId':filterId, 'filterValue':filterValue}
        return filmap

    def tableMapping(self, table, threshold, timeUnit):
        tabmap = {"table": table, "threshold": threshold, "timeUnit": timeUnit}
        return tabmap

    def tableOveride(self, agg, table):
        tabover = {'aggregation':agg, 'tableOverride':table}
        return tabover

    def sam_drill_map(self, kpiID, displayName, operator, values):
        smDrillmap = {'type':kpiID, 'displayName': displayName, 'operator': operator, 'values': values}
        return smDrillmap

    def queryMapping(self, id, name, runSql, select, extraConfig):
        queMap =  {
                    "id": "${KPI_ID}",
                    "name": "${KPI_NAME}",
                    "runSql": "${KPI_FORMULA}"
                  }
        if select:
            queMap['select']=select
        if extraConfig:
            queMap['extraConfig']=extraConfig
        return queMap

    def kpiDetail(self, ids, chartType, customSql):
        kpiDetail = {"id": ids, "type": chartType, 'customSql':customSql}
        return kpiDetail

    def hiererchyDropdown(self, accessible, name, fetchQuery, hasHierarchy, runSql, tableName, queryLevels, samDrill_column, cacheId, fetchQuery, manualFilter, extraClause, joinLevels):
        filterHir = {}
        if queryLevels:
            filterHir['queryLevels'] = [queryLevels]
        if samDrill_column:
            filterHir['samDrill'] = {
                                      "condition": "SIMPLE",
                                      "mapping": [
                                        {
                                          "displayName": samDrill_column
                                        }
                                      ]
                                    }
        
        filterHir['id']=''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)) + "_hFilter"

        if accessible:
            filterHir['accessible'] = accessible
        if accessible & cacheId:
            filterHir['cacheId'] = cacheId
        else if accessible & not cacheId:
            filterHir['cacheId'] = filterHir['id']
        filterHir['fetchQuery'] = fetchQuery
        filterHir['hasHierarchy'] = hasHierarchy
        filterHir['name'] = name
        filterHir['runSql'] = runSql
        filterHir['mappableById'] = True
        if not hasHierarchy:
            filterHir['topLevel'] = "All"
        else:
            filterHir['topLevel'] = None
        filterHir['type'] = "FILTER"
        if tableName:
            filterHir['tableName'] = tableName
        if fetchQuery:
            filterHir['fetchQuery'] = fetchQuery
        if manualFilter:
            filterHir['data'] = [{"id": "All",
                                  "name": "All",
                                  "listOfChildren": [
                                      {"id":item.strip() ,
                                       "name":item.strip() 
                                      } for item in manualFilter.split(",")
                                  ]}]

        if extraClause:
            filterHir['extraClause'] = [extraClause]
        if joinLevels:
            filterHir['joinLevels'] = [joinLevels]
        return filterHir

    def filterComposer(self, ids, dispalynames):
        flcom = {}
        flcom['id']=''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)) + "_filterCom"
        flcom['type'] = "FILTERCOMPOSER"
        flcom['linkedIds'] = [item for item in ids]
        flcom['samDrill'] = {
                              "condition": "COMPLEX",
                              "mapping": [{"type": item, "displayName": val} for item, val in zip(ids,dispalynames)]
                            }
        return flcom

    def pairFilter(self, ids):
        pair = {
                "id": "pairNes",
                "type": "PAIR",
                "linkedIds": [item for item in ids],
                "samDrill": {
                  "condition": "COMPLEX"
                },
                "split": ">>"
              }
        return pair

    def InputFilter(self, name, runSql, operator, samDrill_column):
        InputFilter['id'] = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5))
        InputFilter['type'] = "INPUT_FILTER"
        InputFilter['name'] = name
        InputFilter['runSql'] = runSql
        InputFilter['operator'] = operator
        InputFilter['samDrill'] = {
                                      "condition": "SIMPLE",
                                      "mapping": [
                                        {
                                          "displayName": samDrill_column,
                                            "operator": operator
                                        }
                                      ]
                                    }
        return InputFilter

    def threshold(self, runSql, value, color, justAComment):
        threshold['id'] = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5))
        threshold['filterId'] = threshold['id']
        threshold['runSql'] = runSql
        threshold['thresholds'] = [{"value": v, "color": c, "justAComment": j} for v, c, j in zip(value,color, justAComment)]
        return threshold

    def group_by_data(self, name, runSql, table, correspondingFilter, display_col, join_from_enrichment, join_from_agg, drilldown, drillup):
        grpdata = {
            "id": name,
            "name": name,
            "correspondingFilter": correspondingFilter
        }
        if join_from_enrichment:
            grpdata["runSql"] = display_col.strip()
            grpdata["hierarchyGroupByConfiguration"] = {
                                                  "joinTable": table,
                                                  "joinColumn": join_from_agg,
                                                  "columnToJoin": join_from_enrichment
                                                }
            grpdata["useJoinForDrill"] = True
        else:
            grpdata["runSql"] = name
        if drilldown:
            grpdata["hierarchyDrillDown"] = drilldown
        if drillup:
            grpdata["hierarchyDrillUp"] = drillup

        return grpdata

    def group_by_sam(self, groupbyid, display_col):
        group_by = {}
        group_by["type"] = groupbyid
        group_by["displayName"] = display_col
        return group_by

    def GroupBY(self, context, cdr_map):
        GroupBYOb['id'] = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(5))
        GroupBYOb['type'] = "GROUP_BY"
        GroupBYOb['name'] = "Analyze By"
        GroupBYOb['data'] = []
        for item in context:
            GroupBYOb['data'].append(self.group_by_data(name, runSql, table, correspondingFilter, display_col, join_from_enrichment, join_from_agg, drilldown, drillup))
        if cdr_map:
            GroupBYOb['samDrill'] = {
                                  "condition": context.condition,
                                  "cdrTableMapping": {
                                    "sessionType": context.sessionType,
                                    "protocolSplit": context.protocolSplit
                                  }
        GroupBYOb['samDrill']["mapping"] = []
        GroupBYOb['samDrill']["mapping"].append(self.group_by_sam(groupbyid, display_col))
        GroupBYOb["queryMapping"] = [
                          {
                            "id": "${GROUPBY_ID}",
                            "name": "${GROUPBY_ALIAS}",
                            "runSql": "${GROUPBY_VALUE}",
                            "select": "${GROUPBY_COLUMN}"
                          }
                        ]




class LoadKPIJson:
    def __init__(self, jsonFilepath):
        # Opening JSON file
        with open(jsonFilepath) as json_file:
            self.data = json.load(json_file)

    def getItemObject(self, typeObj):
        listob = []
        for items in range(len(self.data)):
            if self.data[items].get('type') == typeObj:
                listob.append(self.data[items])
        return listob

    def createKPIDictionary(self, KPI_Data):
        for kpi in KPI_Data:
            kpi_dict = {}
            for jsonColumn, excelColumn in conf.get('kpi').items():
                if excelColumn:
                    li = []
                    for kpi_details in kpi.get('data'):
                        li.append(kpi_details.get(jsonColumn))
                    kpi_dict[excelColumn] = li
        return pd.DataFrame.from_dict(kpi_dict)

class createKPIJson:
    def __init__(self, excelFilepath):
        # Opening JSON file
        self.kpi_data = pd.read_excel(excelFilepath, sheet_name=['KPI'])
        self.kpi_df = self.kpi_data['KPI']

    def is_string(self, s):
        if isinstance(s, Number):
            return False
        else:
            return True

    def createJSON(self):
        datajson = self.kpi_df.to_dict('records')
        dataItems = []
        for items in datajson:
            kpi_dict = {}
            for jsonColumn, excelColumn in conf.get('kpi').items():
                if excelColumn:
                    if excelColumn == "valueSuffix" and items.get(excelColumn) == "%":
                        kpi_dict["metricType"] = "RATIO"
                    elif excelColumn == "valueSuffix" and self.is_string(items.get(excelColumn)):
                        kpi_dict["metricType"] = "COUNT"
                    if excelColumn == "ID" and self.is_string(items.get(excelColumn)) == False:
                        kpi_dict[jsonColumn] = "".join(items.get("NAM_KPI_Name").split(" "))
                    if self.is_string(items.get(excelColumn)):
                        kpi_dict[jsonColumn] = items.pop(excelColumn)
            dataItems.append(kpi_dict)
        return dataItems
        
if __name__ == "__main__":
    conf = OmegaConf.load('nam_config.yaml')