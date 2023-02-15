import requests
import modules.config as config
from datetime import datetime , timedelta
from collections import defaultdict
from modules.CRUD import insertOrders, update, read , lastUpdateDate

def extractOrders():
  response = requests.request("POST", config.url, headers=config.headers, data=config.payload)
  print(response)
  if str(response) != "<Response [200]>":
    return False
  else:
    jsondata = response.json()
    jsondata = jsondata['Result']
    return jsondata

def treatOrders(jsondata):
  completeorders = []
  for websiteid in config.websiteids:
    for order in jsondata:
        if str(order['WebSiteID']) == str(websiteid):
            order['CreatedDate'] = str(order['CreatedDate'])[6:-10]
            order['CreatedDate'] = str(datetime.fromtimestamp(int(order['CreatedDate'])))
            order['CreatedDate'] = str(datetime.strptime(order['CreatedDate'], "%Y-%m-%d %H:%M:%S") - timedelta(hours=config.timezone_diff))

            order['PaymentStatus'] = config.statusList[str(order['PaymentStatus'])]
            try:
                order['ShipmentStatus'] = config.shipmentStatusList[str(order['ShipmentStatus'])]
            except:
                order['ShipmentStatus'] = str(order['ShipmentStatus'])
            try:
                order['OrderStatusID'] = config.orderStatusIDList[str(order['OrderStatusID'])]
            except:
                order['OrderStatusID'] = str(order['OrderStatusID'])
            try:
                data_set = [{"orderId"    : order['OrderNumber'] , 
                            "creationDate": order['CreatedDate'] , 
                            "status"      : order['PaymentStatus'] ,
                            "paymentNames": order['PaymentMethods'][0]['PaymentInfo']['Alias'],
                            "totalValue"  : order['Total'],
                            "shipmentStatus" : order['ShipmentStatus'],
                            "orderStatusID" : order['OrderStatusID']
                            }]
            except:
                data_set = [{"orderId"    : order['OrderNumber'] , 
                            "creationDate": order['CreatedDate'] , 
                            "status"      : order['PaymentStatus'] ,
                            "paymentNames": None,
                            "totalValue"  : order['Total'],
                            "shipmentStatus" : order['ShipmentStatus'],
                            "orderStatusID" : order['OrderStatusID']
                            }]
            completeorders.extend(data_set)

  return completeorders

def loadList(orders):
    counter = 0
    print('Loading data into Big Query.')
    for order in orders:
        counter = counter + int(insertOrders(order['orderId'] , order['creationDate'] , order['status'] , order['paymentNames'] , order['totalValue'] , order['shipmentStatus'] , order['orderStatusID']))
    lastUpdateDate(str(datetime.today() - timedelta(1))[0:10])
    print('Data was successfully loaded.')
    return counter

def newOrders(counter):
    while counter > 0:
        config.setWhere(counter, counter - 1)
        extractedOrders = extractOrders()
        if extractedOrders == False:
            config.addFailedStore(str(config.storeName))
            print("Extraction error")
        else:
            loadList(treatOrders(extractedOrders))       
        counter = counter - 1
    return 

def updateOrders():
    config.setWhere(15 , 1)
    extractedOrders = extractOrders()
    if extractedOrders == False:
        print("Extraction error")
    else:
        vtexOrders = treatOrders(extractedOrders)
        orderId = ''
        orderUpadateList = defaultdict(list)
        statusList = []
        #cria a consulta com os parametros dos chamados da vtex
        for vtexOrder in vtexOrders:
            orderId = orderId + str("'{}' , ".format(vtexOrder['orderId']))
        orderId = orderId[:-3]
        readCondition = "orderId IN ({})".format(orderId)

        #Consulta os chamados o bigquery de acordo com os pedidos da vtex
        try:
            bqOrders = read(config.table_id , readCondition)
            for bqOrder in bqOrders:
                #essa linha filtra e cria uma lista com apenas 1 item, aquele que corresponde ao orderId do bigquery
                vtexOrder = list(filter(lambda x:x["orderId"]==str(bqOrder.orderId),vtexOrders))
                status = str(vtexOrder[0]['status'])
                if (bqOrder.status != status):
                    test = str(status) in orderUpadateList
                    if (test == False):
                        #cria a chave dentro do json
                        orderUpadateList[status] = [bqOrder.orderId]
                        #cria lista de status
                        statusList.append(status)
                    else:
                        #adiciona orderId na chave criada acima
                        orderUpadateList[status].append(bqOrder.orderId)

            for status in statusList:
                updateCondition = "orderId IN ({})".format(str(orderUpadateList[status])[1:-1])
                update("status = '" + status + "'", updateCondition)
        except:
            print('sem pedidos para atualizar')
    return