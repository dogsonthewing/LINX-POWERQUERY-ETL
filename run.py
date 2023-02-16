from main import insert , update
from modules.emailsender import sendEmail
import modules.config as configuration

update()
orderCounter = insert()
if orderCounter != 0:
    sendEmail("""<p>Lojas LINX atualizadas com sucesso - Tabelas GBQ individuais</p>
            <p>Lista de erros: {}</p>
            <p>Pedidos inseridos: {}</p>""".format(configuration.failedStores , str(orderCounter)).encode('utf-8'))
    print('Email enviado')