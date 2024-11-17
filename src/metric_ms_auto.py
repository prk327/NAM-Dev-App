# -------------------------------------- Create KPI JSON ----------------------
# importing the module
import json
import pandas as pd
from omegaconf import OmegaConf
from numbers import Number

class MetricJSON:
    def __init__(self, config):
        # ready the config file
        self.conf = OmegaConf.load(config)
        self.metric_ms = []
        return

    def is_string(self, s):
        if isinstance(s, Number):
            return False
        else:
            return True

    def SheetToJSON(self, SheetName):
        # Opening Excel file
        Sheet = pd.read_excel(self.conf.file.excelPath, sheet_name=[SheetName])
        SheetDF = Sheet[SheetName]
        SheetJSON = SheetDF.to_dict('records')
        return SheetJSON, SheetDF

    def DictToJSONFile(self, dictObj, filepath, ):
        # Convert and write JSON object to file
        with open(filepath, "w") as outfile: 
            json.dump(dictObj, outfile)
        return

    def _innerMap(self, rowItem, jsonColumn, columnDict):
        innerDict = {}
        innerList = []
        if jsonColumn[1:].split("_")[0] == 'dict':
            for key in columnDict.keys():
                if self.is_string(rowItem.get(columnDict.get(key))) and rowItem.get(columnDict.get(key)):
                    innerDict.update({key: rowItem.get(columnDict.get(key))})
            if innerDict:
                return {jsonColumn[1:].split("_")[1]: innerDict}
        if jsonColumn[1:].split("_")[0] == 'list':
            for key in columnDict.keys():
                if self.is_string(rowItem.get(columnDict.get(key))) and rowItem.get(columnDict.get(key)):
                    innerDict.update({key: rowItem.get(columnDict.get(key))})
            if innerDict:
                innerList.append(innerDict)
            if innerList:
                return {jsonColumn[1:].split("_")[1]: innerList}

    def _kpiGroupBy(self, SheetName, groupBy_dict, sheetJsonName, idname):
        GroupBy_Filters, _ = self.SheetToJSON(SheetName)
        groupByData = []
        for items in GroupBy_Filters:
            grpDict = {}
            for jsonColumn, excelColumn in self.conf.get(sheetJsonName).items():
                if self.is_string(items.get(excelColumn)) and items.get(excelColumn) and not jsonColumn.startswith("_"):
                    grpDict.update({jsonColumn: items.get(excelColumn)})
                elif jsonColumn.startswith("_"):
                    grpobj = self._innerMap(items, jsonColumn, excelColumn)
                    if grpobj:
                        grpDict.update(grpobj)
            groupByData.append(grpDict)
        groupBy_dict.update({"data" : groupByData})
        samdrill = self.createSamDrill(SheetName, idname)
        if samdrill:
            samdrill.update({"condition": "SIMPLE"})
            groupBy_dict.update({"samDrill": samdrill})
        return groupBy_dict

    def samfilter(self, items, yamlID):
        map_dict = {}
        grpobj = None
        for jsonColumn, excelColumn in self.conf.get(yamlID).items():
            if self.is_string(items.get(excelColumn)) and items.get(excelColumn) and not jsonColumn.startswith("_"):
                map_dict.update({jsonColumn: items.get(excelColumn)})
            elif jsonColumn.startswith("_"):
                grpobj = self._innerMap(items, jsonColumn, excelColumn)
            if grpobj:
                map_dict.update(grpobj)
        return map_dict

    def createGroupByFilters(self, SheetName):
        GroupBy_Filters, _ = self.SheetToJSON(SheetName)
        groupByData = []
        for items in GroupBy_Filters:
            grpDict = {"mappableById": True, "topLevel": "All","type": "FILTER"}
            for jsonColumn, excelColumn in self.conf.get('filter').items():
                if self.is_string(items.get(excelColumn)) and items.get(excelColumn) and not jsonColumn.startswith("_"):
                    grpDict.update({jsonColumn: items.get(excelColumn)})
                elif jsonColumn.startswith("_"):
                    grpobj = self._innerMap(items, jsonColumn, excelColumn)
                    if grpobj:
                        grpDict.update(grpobj)
            samdrill = self.samfilter(items, 'samDrill')
            if samdrill:
                grpDict.update({"samDrill": samdrill})
            groupByData.append(grpDict)
        return groupByData

    def createSamDrill(self, SheetName, IDTypeName):
        samDrill, _ = self.SheetToJSON(SheetName)
        typeID = ""
        mapping = []
        cdrs = []
        cdrTableMapping = []
        drillfunc = {}
        for items in samDrill:
            map_dict = {}
            typeID = items.get(IDTypeName)
            for jsonColumn, excelColumn in self.conf.get('samDrill').items():
                if self.is_string(items.get(excelColumn)) and items.get(excelColumn) and not jsonColumn.startswith("_"):
                    map_dict.update({jsonColumn: items.get(excelColumn)})
                elif jsonColumn.startswith("_"):
                    grpobj = self._innerMap(items, jsonColumn, excelColumn)
                    if grpobj:
                        if str(list(grpobj.keys())[0]) == 'cdrs':
                            cdrs.append(list(grpobj.values())[0][0])
                        if str(list(grpobj.keys())[0]) == 'cdrTableMapping':
                            cdrTableMapping.append(list(grpobj.values())[0][0])
                        if str(list(grpobj.keys())[0]) == 'mapping':
                            mapp = {"type": typeID}
                            mapp.update(list(grpobj.values())[0][0])
                            mapping.append(mapp)
                    if cdrs:
                        drillfunc.update({"cdrs": cdrs})
                    if cdrTableMapping:
                        drillfunc.update({"cdrTableMapping": cdrTableMapping})
                    if mapping:
                        drillfunc.update({"mapping": mapping})
        return drillfunc

    def createKPI(self, SheetName):
        groupBy_dict = {"queryMapping": [
                          {
                                    "id": "${KPI_ID}",
                                    "name": "${KPI_NAME}",
                                    "runSql": "${KPI_FORMULA}"
                                }
                        ],"type": "KPI"}
        kpi = self._kpiGroupBy(SheetName, groupBy_dict, 'kpi', "filterID")
        kpd = [
                      {
                        "id": "trend",
                        "type": "TREND"
                      },
                      {
                        "id": "rank",
                        "type": "RANK",
                        "customSql": "rankCustomSqlCp"
                      }
                    ]
        kpi.update({"kpiDetails": kpd})
        GroupBy_Filters, _ = self.SheetToJSON('Package_Detail')
        idd = [items.get('KPI_ID') for items in GroupBy_Filters if items.get('KPI_ID') is not None][0]
        kpi.update({"id": idd})
        return kpi
        

    def createGroupBy(self, SheetName):
        groupBy_dict = {"name": "Analyze By", "queryMapping": [
              {
                "id": "${GROUPBY_ID}",
                "name": "${GROUPBY_ALIAS}",
                "runSql": "${GROUPBY_VALUE}",
                "select": "${GROUPBY_COLUMN}"
              }
            ],"type": "GROUP_BY"}
        groupBy = self._kpiGroupBy(SheetName, groupBy_dict, 'groupBy', "groupByID")
        GroupBy_Filters, _ = self.SheetToJSON('Package_Detail')
        idd = [items.get('GroupBy_ID') for items in GroupBy_Filters if items.get('GroupBy_ID') is not None][0]
        groupBy.update({"id": idd})
        return groupBy

    def TableMapping(self, SheetName, yamlID):
        GroupBy_Filters, _ = self.SheetToJSON(SheetName)
        dfc = {}
        tableMapping = []
        tableMappingOverride = []
        for items in GroupBy_Filters:
            map_dict = self.samfilter(items, yamlID)
            if map_dict.get('tableMapping'):
                tableMapping.append(map_dict.get('tableMapping')[0])
            if map_dict.get('tableMappingOverride'):
                tableMappingOverride.append(map_dict.get('tableMappingOverride')[0])
        dfc.update({'tableMapping': tableMapping})
        dfc.update({'tableMappingOverride': tableMappingOverride})
        return dfc

    def FilterMapping(self, SheetName='KPI', yamlID='filterMapping'):
        GroupBy_Filters, _ = self.SheetToJSON(SheetName)
        defaultFilters = self.createGroupByFilters('Default_Filters')
        filmap = []
        defFil = []
        for items in GroupBy_Filters:
            map_dict = self.samfilter(items, yamlID)
            if map_dict:
                fid = {"id" : map_dict['filterId']}
                fv = {"filterValue": fid}
                map_dict.update(fv)
                filmap.append(map_dict)
        if defaultFilters:
            for items in filmap:
                for defil in defaultFilters:
                    objdef = {
                        'kpiId': items.get('kpiId'),
                        'filterId': defil.get('id'),
                        "filterValue":{"id" : defil.get('id')}
                    }
                    if defil.get('id'):
                        defFil.append(objdef)
        return {"filterMapping": filmap + defFil}

    def createKPIOBJ(self):
        tabmapping = self.TableMapping('AGG_Table', 'tableMap')
        kpi_dict = self.createKPI('KPI')
        # create kpi obj
        kpi_dict.update(tabmapping)
        filmap = self.FilterMapping()
        kpi_dict.update(filmap)
        return kpi_dict

    def createGroupByOBJ(self):
        groupByData = self.createGroupBy("GroupBy_Filters")
        return groupByData

    def createFilterOBJ(self):
        kpiFilters = self.createGroupByFilters('KPI')
        defaultFilters = self.createGroupByFilters('Default_Filters')
        # default = defaultFilters + kpiFilters
        GroupBy_Filters = self.createGroupByFilters('GroupBy_Filters')
        # defaultFilters.append(GroupBy_Filters)
        default = defaultFilters + kpiFilters + GroupBy_Filters
        # out = reduce(operator.concat, defaultFilters)
        return default

    def createDimentionMetric(self):
        KPIOBJ = self.createKPIOBJ()
        GroupByOBJ = self.createGroupByOBJ()
        FilterOBJ = self.createFilterOBJ()
        FilterOBJ.append(KPIOBJ)
        FilterOBJ.append(GroupByOBJ)
        self.DictToJSONFile(FilterOBJ, self.conf.file.DimentionMetric)
        return
    

if __name__ == "__main__":
    metricSET = MetricJSON('nam_config.yaml')
    metricSET.createDimentionMetric()