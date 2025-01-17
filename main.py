import streamlit as st
import requests
import pandas as pd
import time
import os
import json
import time
from datetime import datetime
from trycourier import Courier
from dotenv import load_dotenv
import pytz
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')

# Carregar variáveis de ambiente
load_dotenv()

# Obter variáveis de ambiente
MONGO_URI = os.getenv('MONGO_URI')
COURIER_API_TOKEN = os.getenv('COURIER_API_TOKEN')
EMAIL_DESTINATARIOS = os.getenv('EMAIL_DESTINATARIOS').split(',')

# Conectar ao cliente MongoDB
client = MongoClient(MONGO_URI, server_api=ServerApi('1'))

db = client["leiloes_judiciais"]
dados_gerais_collection = db["dados_gerais"]
lotes_collection = db["lotes"]

# -------------------- Configurações Iniciais --------------------
API_URL = "https://leilojus-api.tjdft.jus.br/public/leiloes"
DEFAULT_PARAMS = {
    "tiposDeBemALeiloar": "IMOVEL",
    "size": 500,  # Página inicial com 10 itens por página
    "sort": "primeiraHasta,asc",
}
JSON_FILE = "leiloes_data.json"

# -------------------- Funções Auxiliares --------------------
def fetch_leiloes(page=0, additional_params=None):
    """Busca dados da API com paginação e filtros."""
    params = DEFAULT_PARAMS.copy()
    if additional_params:
        params.update(additional_params)
    params["page"] = page

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao buscar dados: {e}")
        return None

def save_to_json(data, filename):
    """Salva dados em arquivo JSON, incluindo informações gerais."""
    dados_gerais = {
        "data_atualizacao": datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Adiciona a seção de dados gerais e os lotes
    final_data = {
        "dados_gerais": dados_gerais,
        "lotes": data
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
        

def load_from_json(filename):
    """Carrega dados de arquivo JSON."""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_to_mongo(data):
    
    dados_gerais = {
        "data_atualizacao": datetime.now(fuso_horario_brasil).strftime('%Y-%m-%d %H:%M:%S')
    }
    
    dados_gerais_collection.update_one(
        {"_id": "dados_gerais"}, 
        {"$set": dados_gerais}, 
        upsert=True
    )
    
    # Salva os lotes na collection 'lotes'
    for item in data:
        lotes_collection.update_one(
            {"id": item["id"]}, 
            {"$set": item}, 
            upsert=True
        )
        
def load_from_mongo():
    """Carrega dados das collections 'dados_gerais' e 'lotes' do MongoDB."""
    
    # Carrega os dados gerais da collection 'dados_gerais'
    dados_gerais = dados_gerais_collection.find_one({"_id": "dados_gerais"})
    
    # Carrega os lotes da collection 'lotes'
    lotes = list(lotes_collection.find())
    
    return {
        "dados_gerais": dados_gerais if dados_gerais else {},
        "lotes": lotes if lotes else []
    }


def format_date(date_str):
    """Formata datas no padrão dd/mm/yyyy hh:mm."""
    if date_str:
        return pd.to_datetime(date_str).strftime('%d/%m/%Y %H:%M')
    return ""

def compare_dicts(dict1, dict2, ignore_fields=None):
    if ignore_fields is None:
        ignore_fields = []
        
    changes = {}

    # Verificar as chaves de ambos os dicionários
    for key in dict1:
        # Ignorar campos especificados
        if key in ignore_fields:
            continue
        
        value1 = dict1[key]
        value2 = dict2.get(key, None)  # Se a chave não existe em dict2, o valor é None

        # Se ambos os valores são dicionários, chama recursivamente
        if isinstance(value1, dict) and isinstance(value2, dict):
            nested_changes = compare_dicts(value1, value2, ignore_fields)
            if nested_changes:  # Se houver mudanças, adiciona ao dicionário de mudanças
                changes[key] = nested_changes
        # Se ambos os valores são listas, chama recursivamente
        elif isinstance(value1, list) and isinstance(value2, list):
            # Compara listas, elemento por elemento
            if len(value1) != len(value2):
                changes[key] = {"old": value1, "new": value2}
            else:
                for i in range(len(value1)):
                    nested_changes = compare_dicts(value1[i], value2[i], ignore_fields)
                    if nested_changes:
                        changes[key] = nested_changes
        # Caso contrário, compara os valores diretamente
        elif value1 != value2:
            changes[key] = {"old": value1, "new": value2}
    
    return changes

def check_for_changes(existing, new, ignore_fields=None):
    changes = []
    
    for existing_item in existing:
        matching_item = next((item for item in new if item['id'] == existing_item['id']), None)
        if matching_item:
            item_changes = compare_dicts(existing_item, matching_item, ignore_fields)  # Verifica se houve mudança
            if item_changes:  # Se houver mudanças, adiciona ao resultado
                # Cria uma cópia do matching_item para evitar alteração no original
                updated_item = matching_item.copy()

                # Verifica se o existing_item já possui historico_alteracoes
                if 'historico_alteracoes' in existing_item:
                    # Copia o histórico de alterações do existing_item para o updated_item
                    updated_item['historico_alteracoes'] = existing_item['historico_alteracoes']
                else:
                    # Se não tiver histórico, inicializa com uma lista vazia
                    updated_item['historico_alteracoes'] = []

                # Adiciona as mudanças no histórico (sem criar um novo nível)
                updated_item['historico_alteracoes'].append({
                    'dataAlteracao': datetime.now(fuso_horario_brasil).isoformat(),
                    'alteracoes': item_changes
                })
                
                # Adiciona o item atualizado ao resultado
                changes.append(updated_item)
    
    return changes

def buscarDados():
    st.info("Buscando novos leilões, por favor aguarde...")
    
    # Reinicia os dados para buscar toda a base
    new_data = []
    all_leiloes = []
    page = 0

    while True:
        leiloes_data = fetch_leiloes(page, additional_params={"status": selected_status})
        if not leiloes_data:
            break

        all_leiloes.extend(leiloes_data)        
        page += 1

    # Filtra os novos leilões que não estão na lista existente
    existing_data_ids = {leilao['id'] for leilao in lotes}
    new_data = [leilao for leilao in all_leiloes if leilao['id'] not in existing_data_ids]

    # Verifica mudanças nos imóveis existentes
    ignore_fields = ["_id","data_atualizacao_api", "historico_alteracoes"] 
    changes = check_for_changes(lotes, all_leiloes,ignore_fields)
    
    
    # Atualiza apenas os leilões modificados
    for leilao in changes:
        lotes_collection.update_one(
            {"id": leilao["id"]}, 
            {"$set": leilao}, 
            upsert=True
        )

    lotes_collection.insert_many(new_data)

    return new_data, changes


def formatar_novos_imoveis_email(json_data):
    formatted_string = ""
    
    for imovel in json_data:
        leiloeiro = imovel.get("leiloeiro", {})
        processo = imovel.get("processo", {})
        bens = imovel.get("bensALeiloar", [])
        
        formatted_string += (
            f"\nID do Leilão: {imovel.get('id')}\n"
            f"Tipo de Leilão: {imovel.get('tipoDeLeilao')}\n"
            f"Primeira Hasta: {imovel.get('primeiraHasta')}\n"
            f"Segunda Hasta: {imovel.get('segundaHasta')}\n"
            f"Status: {imovel.get('status')}\n"
            f"Justificativa (se houver): {imovel.get('justificativaCancelamentoSuspensao', 'N/A')}\n"
            f"Valor Total dos Bens: R$ {imovel.get('valorTotalBens', 0):,.2f}\n"
            f"----- PROCESSO -----\n"
            f"Número do Processo: {processo.get('numeroProcessoFormatado')}\n"
            f"Polo Ativo: {processo.get('poloAtivo')}\n"
            f"Polo Passivo: {processo.get('poloPassivo')}\n"
            f"Órgão Julgador: {processo.get('orgaoJulgador', {}).get('nome', 'N/A')}\n"
            f"--- LEILOEIRO ---\n"
            f"Site: {leiloeiro.get('localRealizacao')}\n"
        )
        
        formatted_string += "----- BENS A LEILOAR -----\n"
        for bem in bens:
            formatted_string += (
                f"Descrição: {bem.get('descricao', 'N/A')}\n"
                f"Valor: R$ {bem.get('valor', 0):,.2f}\n"
                "-----------------------\n"
            )
        
        formatted_string += "\n" + ("=" * 40) + "\n\n"

    return formatted_string

def send_email(subject, body):
    try:
        # Configurar Courier
        client = Courier(auth_token=COURIER_API_TOKEN)
        
         # Cria a lista de destinatários
        to_list = [{"email": email.strip()} for email in EMAIL_DESTINATARIOS if email.strip()]

        if not to_list:
            print("Nenhum destinatário válido encontrado.")
            return
        response = client.send_message(
            message={
                "to": to_list,
                "content": {
                    "title": subject,
                    "body": body,
                }
            }
        )
        
        print(response)

    except Exception as e:
        print(e)
        # Imprimir o erro em caso de exceção
        print(f"Erro ao enviar e-mail: {str(e)}")

# -------------------- Layout da Aplicação --------------------

st.set_page_config(
        page_title="Leilões Judiciais - DF",
        page_icon="⚖️"
    )

st.title("Leilojus - Busca de Leilões")

# -------------------- Carregamento de Dados --------------------
existing_data = load_from_mongo()

if not existing_data:
    st.info("Carregando dados iniciais, por favor aguarde...")

    all_leiloes = []
    page = 0

    while True:
        leiloes_data = fetch_leiloes(page)
        if not leiloes_data:
            break

        all_leiloes.extend(leiloes_data)
        page += 1

    if all_leiloes:
        all_leiloes = {"lotes": all_leiloes}  # Estrutura de lotes
        # save_to_json(all_leiloes, JSON_FILE)
        save_to_mongo(all_leiloes['lotes'])
        st.success(f"Dados iniciais carregados com {len(all_leiloes['lotes'])} registros.")
    else:
        st.warning("Nenhum dado inicial encontrado.")

# existing_data = load_from_json(JSON_FILE)


dados_gerais = existing_data['dados_gerais']
lotes = existing_data['lotes']

st.text(f"Última atualização: {format_date(dados_gerais['data_atualizacao'])}h")

# -------------------- Menu Lateral (Filtros e Paginação) --------------------
st.sidebar.write(f"Dados existentes: {len(lotes)} registros")
st.sidebar.header("Filtros de Busca")
selected_sort = st.sidebar.selectbox(
    "Ordenar por", [
        "Data 1º Leilão, Crescente",
        "Data 1º Leilão, Decrescente",
        "Data de Criação, Crescente",
        "Data de Criação, Decrescente",
        "Data de Atualização, Decrescente",
    ], index=3
)
selected_status = st.sidebar.selectbox("Status do Leilão", ["", "HASTA2_REPORTADA", "HASTA3_REPORTADA", "SUSPENSO", "CANCELADO", "ENCERRADO", "AGENDADO", "HASTA1_NAO_REALIZADA", "ANALISAR_SUSPENSAO_CANCELAMENTO"])
data_inicio = st.sidebar.date_input("Data de início", format="DD/MM/YYYY", value=None)
data_fim = st.sidebar.date_input("Data de fim", format="DD/MM/YYYY", value=None)
endereco_filtro = st.sidebar.text_input("Buscar por Endereço")

st.sidebar.header("Paginação")
page_size = st.sidebar.selectbox("Tamanho da Página", [10, 20, 50, 100, "Todos"], index=1)

# Configuração inicial da página
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1
current_page = st.session_state.current_page

# -------------------- Aplicar Filtros --------------------
filtered_data = lotes
if endereco_filtro:
    filtered_data = [
        d for d in filtered_data
        if any(
            endereco_filtro.lower() in bem["descricao"].lower()
            for bem in d.get("bensALeiloar", [])
        )
    ]
if selected_status:
    filtered_data = [d for d in filtered_data if d.get("status") == selected_status]
if data_inicio:
    filtered_data = [
        d for d in filtered_data
        if pd.to_datetime(data_inicio) <= pd.to_datetime(d.get("primeiraHasta"))
    ]
if data_fim:
    filtered_data = [
        d for d in filtered_data
        if pd.to_datetime(d.get("primeiraHasta")) <= pd.to_datetime(data_fim)
    ]

if selected_sort == "Data 1º Leilão, Decrescente":
    filtered_data.sort(key=lambda x: pd.to_datetime(x["primeiraHasta"]), reverse=True)
elif selected_sort == "Data 1º Leilão, Crescente":
    filtered_data.sort(key=lambda x: pd.to_datetime(x["primeiraHasta"]))
elif selected_sort == "Data de Criação, Crescente":
    filtered_data.sort(key=lambda x: pd.to_datetime(x["processo"]["dataCriacao"]))
elif selected_sort == "Data de Criação, Decrescente":
    filtered_data.sort(key=lambda x: pd.to_datetime(x["processo"]["dataCriacao"]), reverse=True)
elif selected_sort == "Data de Atualização, Decrescente":
    filtered_data.sort(key=lambda x: (
        max((pd.to_datetime(h["dataAlteracao"]).tz_localize(None) for h in x.get("historico_alteracoes", [])), default=pd.Timestamp.min),
        pd.to_datetime(x["processo"]["dataCriacao"]).tz_localize(None)
    ), reverse=True)

else:
    filtered_data.sort(key=lambda x: pd.to_datetime(x["primeiraHasta"]))
# -------------------- Paginação --------------------
total_items = len(filtered_data)
if page_size != "Todos":
    page_size = int(page_size)
    total_pages = (total_items // page_size) + (1 if total_items % page_size > 0 else 0)
    if total_pages > 1:
        current_page = st.sidebar.slider(
            "Escolha a página", 
            min_value=1, 
            max_value=total_pages, 
            value=current_page, 
            step=1
        )
    else:
        current_page = 1
    start_idx = (current_page - 1) * page_size
    end_idx = start_idx + page_size
    page_data = filtered_data[start_idx:end_idx]
else:
    total_pages = 1
    page_data = filtered_data

st.text(f"Página {current_page} de {total_pages}")
st.subheader(f"Leilões Encontrados: {total_items}")

# -------------------- Atualizar Dados --------------------
if st.sidebar.button("Buscar Novos Leilões"):
    new_data, changes = buscarDados()
    st.success(f"{len(new_data)} novos leilões e {len(changes)} leilões atualizados encontrados!")
    time.sleep(3)
    st.rerun()

if st.query_params.get("buscar") == "true":
    new_data, changes = buscarDados()
    st.success(f"{len(new_data)} novos leilões e {len(changes)} leilões atualizados encontrados!")
    
    if(new_data or changes):
        alert_subject = 'Leilões Judiciais DF - Novos Imóveis Adicionados!'
        alert_body = f'Foram adicionados {len(new_data)} novos imóveis e {len(changes)} atualizados. Verifique a lista para mais detalhes:\n\n'
        alert_body += '\n\n--- Novos Imóveis ---\n\n'
        alert_body += formatar_novos_imoveis_email(new_data)
        alert_body += '\n\nConfira em: https://leiloesjusdf.streamlit.app/\n\n'
        send_email(alert_subject, alert_body)

# -------------------- Exibição de Resultados --------------------
if page_data:
    for leilao in page_data:
        primeira_hasta = format_date(leilao.get('primeiraHasta', ''))
        segunda_hasta = format_date(leilao.get('segundaHasta', ''))

        c = st.container(border=True)
        # Gerar o card HTML
        card_html = f"""
        <div>
            <h3>Processo: {leilao['processo']['numeroProcessoFormatado']}</h3>
            <span><strong>Data de Criação:</strong> {format_date(leilao['processo']['dataCriacao'])}</span>
            <p><strong>Status:</strong> {leilao['status']}</p>
            <p>
                <span style="margin-right: 3rem;"><strong>1º Leilão:</strong> {primeira_hasta}</span>
                <span><strong>2º Leilão:</strong> {segunda_hasta}</span>
            </p>
            <p><strong>Valor Total:</strong> R${'{:,.2f}'.format(leilao['valorTotalBens']).replace(',', 'x').replace('.', ',').replace('x', '.')}</p>
            <p><strong>Leiloeiro:</strong> 
                <a target="_blank" style="text-transform: lowercase; color: #837bf3" href="https://{leilao['leiloeiro']['localRealizacao']}">
                    {leilao['leiloeiro']['localRealizacao']}
                </a>
            </p>
        </div>
        """
        # Renderizar no Streamlit
        c.html(card_html)
        
        lista_bens = f"""
         <p><strong>Bens a Leiloar:</strong></p>
            <ul style="list-style-type: disc; margin-left: 50px;">
                {''.join(f"""
                <li style="margin-bottom: 5px;">
                    <strong>Descrição:</strong> {bem['descricao']}<br>
                    <strong>Valor: R${'{:,.2f}'.format(bem['valor']).replace(',', 'x').replace('.', ',').replace('x', '.')}</strong>
                </li>
                </br>
                """ for bem in leilao['bensALeiloar'])}
            </ul>
        """
        
        c.html(lista_bens)

        # Exibir alterações
        with c:
            if 'historico_alteracoes' in leilao and leilao['historico_alteracoes']:
                grouped_changes = {}

                # Agrupar alterações por data
                for alteracao in leilao['historico_alteracoes']:
                    data_alteracao = alteracao.get('dataAlteracao', 'Data desconhecida')
                    if data_alteracao not in grouped_changes:
                        grouped_changes[data_alteracao] = []

                    # Processar alterações, incluindo campos aninhados
                    def process_changes(prefix, changes):
                        for campo, valores in changes.items():
                            # Se os valores forem um dicionário, verificar 'old' e 'new'
                            if isinstance(valores, dict):
                                if 'old' in valores and 'new' in valores:
                                    old_value = valores['old']
                                    new_value = valores['new']
                                    if old_value != new_value:
                                        campo_completo = f"{prefix}{campo}" if prefix else campo
                                        grouped_changes[data_alteracao].append((campo_completo, old_value, new_value))
                                else:
                                    # Explorar níveis mais profundos de aninhamento
                                    process_changes(f"{prefix}{campo}.", valores)
                            else:
                                # Caso inesperado, ignorar ou logar
                                continue

                    process_changes("", alteracao.get('alteracoes', {}))

                sorted_dates = sorted(grouped_changes.keys(), reverse=True)

                # Exibir as alterações agrupadas por data
                with st.expander("Alterações"):
                    if grouped_changes:
                        for data in sorted_dates:
                            st.write(f"**DATA DA ALTERAÇÃO:** {format_date(data)}")
                            for campo, old_value, new_value in grouped_changes[data]:
                                st.write(f"  **{campo}:**  {old_value} ➡️ {new_value}")
                            st.divider()



else:
    st.info("Nenhum leilão encontrado para os filtros selecionados.")
