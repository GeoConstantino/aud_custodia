import os
import pandas as pd
import json
import gspread
import re
import numpy
import time

from oauth2client.service_account import ServiceAccountCredentials
from robotj.extrator.crawler import pipeliner


WORKFLOW = "GOOGLE"
#WORKFLOW = "LOCAL"
#WORKFLOW = "TESTE"


def GeraNumeroCompleto(num):
    num_proc_end = '8190001'
    num_completo = re.sub(r"\-*\/*\.*", "", num) 

    if num_completo.endswith('8190001'):
        return num_completo
    else:
        return str(num_completo + num_proc_end)


def get_sheet_from_google():

    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        'creds.json', scope)
    gc = gspread.authorize(credentials)
    sheet = gc.open_by_url(
        'https://docs.google.com/spreadsheets/d/1CHTuHzJ-xOw5XABA7pfnTtmPI_AhFMbztIUSh995nOY/edit#gid=1576371513'
        )
    return sheet


def get_col(planilha, coluna):

    sheet = get_sheet_from_google()
    worksheet = sheet.get_worksheet(planilha)
    # print(worksheet.range('A1:B2'))
    return (worksheet.col_values(coluna))


def get_wsheet(planilha):

    sheet = get_sheet_from_google()
    worksheet = sheet.get_worksheet(planilha)
    return worksheet


def clear_string(word):
    return re.sub(r"\'*\,*\"*\[*\]*", '', word)


if __name__ == '__main__':

    # desativado por começar a buscar do Google Spreadsheet
    #df = pd.read_csv('ListaProcesso.csv',error_bad_lines=False)
    DadosProcesso = pd.DataFrame(columns=[
                                            'NumeroProcessoCustodia',
                                            'NumeroProcessoCompleto',
                                            'Comarca',
                                            'Endereço',
                                            'Assunto',
                                            'Ação',
                                            'Classe',
                                            'Autor',
                                            'Requerido',
                                            'Requerente',
                                            'Acusado',
                                            'Data da Distribuição'
                                            ])

    ProcessoErro = pd.DataFrame(columns=['NumeroProcessosErro'])



    if WORKFLOW == 'LOCAL':

        print('Executando fluxo Local')

        NumeroProcessosLer = get_col(2, 2)

        NumeroProcessosLido = get_col(3, 2)

        Processos = set(NumeroProcessosLer) - set(NumeroProcessosLido)

        for numero in Processos:

            NumProcCompleto = GeraNumeroCompleto(numero)

            try:
                proc = pipeliner.pipeline(NumProcCompleto)

                getAcusado = clear_string(str(proc.get('reu', ''))) + clear_string(str(proc.get('acusado', ''))) + clear_string(str(proc.get('autor-do-fato', '')))+ clear_string(str(proc.get('denunciado', '')))

                DadosProcesso = DadosProcesso.append({'NumeroProcessoCustodia': str(numero),
                                                      'NumeroProcessoCompleto': str(NumProcCompleto),
                                                      'Comarca': clear_string(str(proc['comarca'])),
                                                      'Endereço': clear_string(str(proc['endereco'])),
                                                      'Assunto': clear_string(str(proc['assunto'])),
                                                      'Ação': clear_string(str(proc['acao'])),
                                                      'Classe': clear_string(str(proc['classe'])),
                                                      'Autor': clear_string(str(proc['autor'])),
                                                      'Requerido': clear_string(str(proc['requerido'])),
                                                      'Requerente': clear_string(str(proc['requerente'])),
                                                      'Acusado': getAcusado,
                                                      'Data da Distribuição': clear_string(str(proc.get('data-da-distribuicao','')))}, ignore_index=True)

            except UnboundLocalError:
                ProcessoErro = ProcessoErro.append(
                    {
                    'NumeroProcessoErro': numero,
                    'NumeroProcessoErroCompleto': NumProcCompleto,
                    'TipoErro': 'UnboundLocalError'
                    }, ignore_index=True)
                pass

            except TypeError:
                ProcessoErro = ProcessoErro.append(
                    {
                    'NumeroProcessoErro': numero,
                    'NumeroProcessoErroCompleto': NumProcCompleto,
                    'TipoErro': 'TypeError'
                    }, ignore_index=True)
                pass

            # Grava localmente a saída dos arquivos
            DadosProcesso.to_csv('DadosProcessoGoogle2.csv', encoding='Latin1', sep=';')
            ProcessoErro.to_csv('ListaErroOutGoogle2.csv', encoding='Latin1', sep=';')


    if WORKFLOW == 'TESTE':

        print('Executando fluxo de TESTES')

        NumeroProcessosLer = get_col(2, 2)

        NumeroProcessosLer = NumeroProcessosLer[-50:-1]

        #NumeroProcessosLido = get_col(3, 2)

        #Processos = set(NumeroProcessosLer) - set(NumeroProcessosLido)

        for numero in NumeroProcessosLer:

            NumProcCompleto = GeraNumeroCompleto(numero)

            try:
                proc = pipeliner.pipeline(NumProcCompleto)

                getAcusado = clear_string(str(proc.get('reu', ''))) + clear_string(str(proc.get('acusado', ''))) + clear_string(str(proc.get('autor-do-fato', '')))+ clear_string(str(proc.get('denunciado', '')))


                DadosProcesso = DadosProcesso.append({'NumeroProcessoCustodia': str(numero),
                                                      'NumeroProcessoCompleto': str(NumProcCompleto),
                                                      'Comarca': clear_string(str(proc['comarca'])),
                                                      'Endereço': clear_string(str(proc['endereco'])),
                                                      'Assunto': clear_string(str(proc['assunto'])),
                                                      'Ação': clear_string(str(proc['acao'])),
                                                      'Classe': clear_string(str(proc['classe'])),
                                                      'Autor': clear_string(str(proc['autor'])),
                                                      'Requerido': clear_string(str(proc['requerido'])),
                                                      'Requerente': clear_string(str(proc['requerente'])),
                                                      'Acusado': getAcusado,
                                                      'Data da Distribuição': clear_string(str(proc.get('data-da-distribuicao','')))}, ignore_index=True)

            except UnboundLocalError:
                ProcessoErro = ProcessoErro.append(
                    {
                    'NumeroProcessoErro': numero,
                    'NumeroProcessoErroCompleto': NumProcCompleto,
                    'TipoErro': 'UnboundLocalError'
                    }, ignore_index=True)
                pass

            except TypeError:
                ProcessoErro = ProcessoErro.append(
                    {
                    'NumeroProcessoErro': numero, 
                    'NumeroProcessoErroCompleto': NumProcCompleto, 
                    'TipoErro': 'TypeError'
                    }, ignore_index=True)
                pass

            # Grava localmente a saída dos arquivos
        DadosProcesso.to_csv('teste_DadosProcesso1910.csv', encoding='Latin1', sep=';')
        ProcessoErro.to_csv('teste_ListaErro1910.csv', encoding='Latin1', sep=';')


    elif WORKFLOW == 'GOOGLE':

        print('Executando fluxo do Google Spreadsheets')

        NumeroProcessosLer = get_col(2, 2)

        NumeroProcessosLido = get_col(3, 2)
        # Temporário -> Pegando 1000 últimos
        NumeroProcessosLer = NumeroProcessosLer[-1000:-1]

        Processos = set(NumeroProcessosLer) - set(NumeroProcessosLido)

        worksheet = get_wsheet(3)
          # recebe a planilha inteira
        wsErros = get_wsheet(4)

        for numero in Processos:

            time.sleep(1.1)

            NumProcCompleto = GeraNumeroCompleto(numero)

            try:
                proc = pipeliner.pipeline(NumProcCompleto)

                getAcusado = clear_string(str(proc.get('reu', ''))) + clear_string(str(proc.get('acusado', ''))) + clear_string(str(proc.get('autor-do-fato', '')))+ clear_string(str(proc.get('denunciado', '')))

                worksheet.append_row([
                    str(numero),
                    str(NumProcCompleto),
                    clear_string(str(proc['comarca'])),
                    clear_string(str(proc['endereco'])),
                    clear_string(str(proc['assunto'])),
                    clear_string(str(proc['acao'])),
                    clear_string(str(proc['classe'])),
                    clear_string(str(proc['autor'])),
                    clear_string(str(proc['requerido'])),
                    clear_string(str(proc['requerente'])),
                    clear_string(str(getAcusado)),
                    clear_string(str(proc.get('data-da-distribuicao','')))
                ])

            except Exception as error:
                wsErros.append_row(
                    [numero, NumProcCompleto, 'Número Incorreto'])

            #except UnboundLocalError:
                #wsErros.append_row(
                    #[numero, NumProcCompleto, 'UnboundLocalError'])
                

            except TypeError:
                wsErros.append_row([numero, NumProcCompleto, 'TypeError'])
