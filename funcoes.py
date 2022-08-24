# Renomeia as colunas, pois o "." no pandas é utilizado para acessar a coluna específica
def rename_columns_dataframe(df:pd.DataFrame)-> pd.DataFrame:

  df.columns = df.columns.str.replace(".","_")
  df.columns = df.columns.str.lower()
  
  return df

# Identificar os duplicados em um dataframe
def print_duplicates(df:pd.DataFrame, nome_df:str)->None:

  df_duplicados = df[df.duplicated(keep=False)].sum().reset_index(name='n_duplicados').rename(columns={'index':'colunas'})
  print(f"----{nome_df}----\n{df_duplicados.query('n_duplicados > 0.0')}")

# Transforma as colunas de data no tipo datetime, a função recebe um dataframe e uma lista de colunas que tem essa propriedade
def transform_to_date_columns(df:pd.DataFrame, list_columns:list)->pd.DataFrame:

  for column in list_columns:
    df[column] = pd.to_datetime(df[column])
  
  return df

# Vai printar as colunas que estão null de acordo com um treshold específico
def print_pct_null_columns(df:pd.DataFrame, trashold:float)->None:

  nan = df.isnull().sum().reset_index(name='nulls').rename(columns={'index':'colunas'})
  nan['pct_null'] = ( nan['nulls'] / len(df) ) * 100

  columns =  nan[nan["pct_null"] > trashold ]['colunas'].to_list()

  print(f"Colunas que podem ser excluídas:\n{columns}")

# Função para excluir colunas
def remove_columns(df:pd.DataFrame, list_columns:list)->pd.DataFrame:
  
  df = df.drop(columns=list_columns)

  return df 

# Faz o levantamento de alunos antes e depois do ENEM e retorna somente os dados a partir de 2011
def treatment_before_after_enem(df:pd.DataFrame)->pd.DataFrame:

  before = df.query("ano_de_admissao < 2011 ").groupby(by=["cod_curso",'descricao_situacao_aluno'])['mat_id'].count().reset_index().rename(columns={'mat_id':'before_enem'})
  after = df.query("ano_de_admissao >= 2011 ").groupby(by=["cod_curso",'descricao_situacao_aluno'])['mat_id'].count().reset_index().rename(columns={'mat_id':'after_enem'})

  result = pd.merge(before, after , on=['cod_curso', 'descricao_situacao_aluno'], how='outer' )
  result = result.replace(np.nan, 0 )
  
  result = result.append({'cod_curso' : 'total' , 'before_enem' : result['before_enem'].sum(), 'after_enem' : result['after_enem'].sum()} , ignore_index=True)
  
  result["pct_before"] = ( result['before_enem'] * 100 ) / len(before)
  result["pct_after"] = ( result['after_enem'] * 100 ) / len(after)

  print(result)

  return  df.query("ano_de_admissao >= 2011")

# Função que calcula o período que o aluno cursou a disciplina
# ex: AEDS é do período 2, entretanto o aluno cursou a disciplina após 5 períodos que ele estava na Universidade
# criada no sentido: o aluno que faz a disciplina no período que a disciplina foi planejada, tem mais sucesso?
# entretanto as grades mudam ao longo do tempo, e eu não consegui acesso a matrizes curriculares históricas

def calcular_periodo_que_cursou_a_disciplina(df_data:pd.DataFrame, df_grade:pd.DataFrame) -> pd.DataFrame:
  
  df_aux = df_data[["mat_id", "ano_de_admissao","semestre_de_admissao"]].copy()
  df_grade = pd.merge(df_grade, df_aux, how="inner", on=['mat_id'])

  df_grade = df_grade.astype({"ano_de_admissao": int})
  df_grade = df_grade.astype({"semestre_de_admissao": int})
  # alterando para deixar em formato adequado para realizar as contas -> 17.2  18.1
  df_grade['periodo_que_entrou'] =  df_grade[['ano_de_admissao', 'semestre_de_admissao']].astype(str).apply('.'.join, axis=1)
  df_grade = df_grade.astype({"periodo_que_entrou": float})

  df_grade['periodo_que_cursou'] =  df_grade[['ano', 'semestre']].astype(str).apply('.'.join, axis=1)
  df_grade.loc[df_grade['periodo_que_cursou'] == '2020.3', 'tag_online'] = '1'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2021.1', 'tag_online'] = '1'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2021.2', 'tag_online'] = '1'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2020.3', 'periodo_que_cursou'] = '2020.2'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2018.4', 'periodo_que_cursou'] = '2018.2'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2019.4', 'periodo_que_cursou'] = '2019.2'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2011.3', 'periodo_que_cursou'] = '2011.2'
  df_grade.loc[df_grade['periodo_que_cursou'] == '2014.3', 'periodo_que_cursou'] = '2014.2'

  df_grade = df_grade.astype({"periodo_que_cursou": float})

  df_referencia_periodos = df_grade.groupby(by=["mat_id", 'periodo_que_entrou'] )['periodo_que_cursou'].value_counts().reset_index(name='count').drop(columns=['count'])
  df_referencia_periodos = df_referencia_periodos.sort_values(by=['mat_id','periodo_que_entrou', 'periodo_que_cursou'])

  df_referencia_periodos['aux_periodo_que_cursou'] = df_referencia_periodos['periodo_que_cursou'].copy()
  df_referencia_periodos['aux_periodo_que_entrou'] = df_referencia_periodos['periodo_que_entrou'].copy()
  df_referencia_periodos['aux_periodo_que_entrou'] = 0

  result_count_periodo = pd.DataFrame()
  for mat_id in df_referencia_periodos.mat_id.unique():


    aux = df_referencia_periodos.query("mat_id == @mat_id").reset_index(drop=True).sort_values(by=['mat_id','periodo_que_entrou', 'periodo_que_cursou'])
    
    list_all_periodos = list(aux.periodo_que_cursou.sort_values(ascending=True).unique()) 
    
    df_aux_base = pd.DataFrame(list_all_periodos, columns=['periodos_base'])

    aux.loc [ aux['aux_periodo_que_cursou'] == df_aux_base['periodos_base'] , 'aux_periodo_que_cursou' ] =  aux[aux['aux_periodo_que_cursou'] == df_aux_base['periodos_base']].index

    aux['periodo_que_fez_a_disciplina'] = aux['aux_periodo_que_cursou'] - aux['aux_periodo_que_entrou']
    aux['periodo_que_fez_a_disciplina'] = aux['periodo_que_fez_a_disciplina'].apply(lambda x: 1 if x == 0 else x + 1)

    result_count_periodo = result_count_periodo.append(aux[['mat_id','periodo_que_cursou','periodo_que_fez_a_disciplina']])

  result = pd.merge(df_grade, result_count_periodo, on=['mat_id', 'periodo_que_cursou'], how='left')
  
  return result

# Função utilizada para marcar se o aluno passou do período normal do curso ou não
def flag_tempo_de_matriculado( df_data:pd.DataFrame )->pd.DataFrame:

  df_data.loc[ df_data.query("cod_curso in  ['PJM', 'CJM', 'EJM']").index.to_list(), 'n_semestres_total_curso' ] = 10
  df_data.loc[ df_data.query("cod_curso == 'SJM'").index.to_list(), 'n_semestres_total_curso' ] = 8

  df_data['n_semestres_total_curso'] = df_data['n_semestres_total_curso'].astype(int)
  df_data['max_n_periodos'] = df_data['max_n_periodos'].astype(int)

  df_data.loc[ df_data.query(" max_n_periodos > n_semestres_total_curso").index.to_list(), 'flag_situacao'] = 'passou_do_tempo'
  df_data.flag_situacao = df_data.flag_situacao.replace(np.nan, 'no_tempo')

  return df_data

# Marca se a cidade de nascimento do aluno é uma cidade próxima à joão monlevade,
# criada no sentido de: ir e voltar da faculdade gera algum impacto na educação do aluno?
def mark_city_limity(df_data:pd.DataFrame)->pd.DataFrame:
  
  list_municipios = ["JOAO MONLEVADE","ITABIRA", "BELA VISTA DE MINAS", "SAO GONCALO DO RIO ABAIXO", "RIO PIRACICABA", "JOAO MONLEVADE", "ALVINOPOLIS", "BARAO DE COCAIS", "SAO DOMINGOS DO PRATA"
                    "NOVA ERA", "SANTA BARBARA"]

  df_data["regiao_proxima"] = df_data["cidade_nascimento"]
  df_data["regiao_proxima"] = df_data.regiao_proxima.apply(lambda x : 1 if x in list_municipios else 0)

  df_data.loc[df_data['cidade_nascimento'].isnull(), "cidade_nascimento"] = "JOAO MONLEVADE"

  return df_data

# Trata os alunos que não fizeram o vertibular, transformando o null para 0
def treat_position_entrance(df:pd.DataFrame)->pd.DataFrame:

  df.loc[df['classificacao_vestibular'].isnull(), "classificacao_vestibular"] = 0
  df.loc[df['pontuacao_vestibular'].isnull(), "pontuacao_vestibular"] = 0

  return df

# Calcula a porcentagem de carga horária cursada do aluno
def pct_cursado(df_data:pd.DataFrame)->pd.DataFrame:
  
  df_data['pct_cursado'] = ( df_data['carga_horaria_cursada'] * 100 ) /  df_data['carga_horaria_curso']
  df_data = df_data.drop(columns=['carga_horaria_cursada','carga_horaria_curso'])
  df_data.loc[ df_data.pct_cursado.isnull() == True , 'pct_cursado'] = 0

  return df_data

# Calcula a idade que o aluno entrou na universidade
def calculate_age(df:pd.DataFrame)->pd.DataFrame:

  df["idade_que_entrou"] = (df.ano_de_admissao - df.ano_nascimento)
  
  return df

# Cria uma coluna nova, para agregar os o grupo de alunos que foram reprovados por algum motivo, como um crupo único de reprovação
def create_situation_class(df:pd.DataFrame)->pd.DataFrame:

  df.loc[df["situacao"] == 'REPROVADO POR NOTA', "situacao_agrupada" ] = "REPROVADO"
  df.loc[df["situacao"] == 'REPROVADO NOTA E FALTA', "situacao_agrupada" ] = "REPROVADO"
  df.loc[df["situacao"] == 'REPROVADO POR FALTA', "situacao_agrupada" ] = "REPROVADO"
  df.loc[df["situacao"] == 'TRANCADO', "situacao_agrupada" ] = "TRANCADO"
  df.loc[df["situacao"] == 'CANCELADO', "situacao_agrupada" ] = "CANCELADO"
  df.loc[df["situacao"] == 'APROVADO', "situacao_agrupada" ] = "APROVADO"

  return df

# Calcula a diferença de nota, ou seja, se o aluno passou com nota sobrando o saldo dele é positivo,
# se ele não atingiu 60 ele tem um saldo negativo
def calculate_diff_value_to_pass(df_grade:pd.DataFrame)->pd.DataFrame:

  df_grade['diff_nota'] = df_grade['media_final'] - 6.0
  df_grade.loc[ (df_grade['situacao'] == 'CANCELADO') | (df_grade['situacao'] == 'TRANCADO'), 'diff_nota' ] = np.nan

  return df_grade

# Marca se o aluno fez o exame especial ou não
def tratando_exame_especial(df_grade:pd.DataFrame)->pd.DataFrame:
  
  df_grade.loc[df_grade[df_grade["exame_especial"].isna() == False ].index , "exame_especial"] = 1 
  df_grade.loc[df_grade[df_grade["exame_especial"].isna() == True ].index , "exame_especial"] = 0

  return df_grade

# Faz o calculo de quantas disciplinas o aluno cursou por período
def count_disciplinas_por_periodo(df_grade:pd.DataFrame)->pd.DataFrame:

  ap = df_grade.query("situacao_agrupada == 'APROVADO'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_de_disciplinas_ap_no_periodo')
  rp = df_grade.query("situacao_agrupada == 'REPROVADO'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_de_disciplinas_rp_no_periodo')

  result = pd.merge(ap, rp, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  tr = df_grade.query("situacao_agrupada == 'TRANCADO'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_de_disciplinas_tr_no_periodo')

  result = pd.merge(result, tr, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  ca = df_grade.query("situacao_agrupada == 'CANCELADO'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_de_disciplinas_ca_no_periodo')

  result = pd.merge(result, ca, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  result['qnt_de_disciplinas_ap_no_periodo'] = result['qnt_de_disciplinas_ap_no_periodo'].fillna(0)
  result['qnt_de_disciplinas_rp_no_periodo'] = result['qnt_de_disciplinas_rp_no_periodo'].fillna(0)
  result['qnt_de_disciplinas_ca_no_periodo'] = result['qnt_de_disciplinas_ca_no_periodo'].fillna(0)
  result['qnt_de_disciplinas_tr_no_periodo'] = result['qnt_de_disciplinas_tr_no_periodo'].fillna(0)

  df_grade = pd.merge(df_grade, result , how='left', on=['mat_id', "periodo_que_fez_a_disciplina"])

  return df_grade

# Faz o cálculo de quantas disciplinas os alunos se matricularam por departamento
def count_departamento_por_periodo(df_grade:pd.DataFrame)->pd.DataFrame:

  decea = df_grade.query("cod_departamento == 'DECEA'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_departamento_decea')
  
  decsi = df_grade.query("cod_departamento == 'DECSI'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_departamento_decsi')

  result = pd.merge(decea, decsi, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  deelt = df_grade.query("cod_departamento == 'DEELT'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_departamento_deelt')

  result = pd.merge(result, deelt, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  deenp = df_grade.query("cod_departamento == 'DEENP'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_departamento_deenp')

  result = pd.merge(result, deenp, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  result['qnt_departamento_deelt'] = result['qnt_departamento_deelt'].fillna(0)
  result['qnt_departamento_decea'] = result['qnt_departamento_decea'].fillna(0)
  result['qnt_departamento_decsi'] = result['qnt_departamento_decsi'].fillna(0)
  result['qnt_departamento_deenp'] = result['qnt_departamento_deenp'].fillna(0)

  df_grade = pd.merge(df_grade, result , how='left', on=['mat_id', "periodo_que_fez_a_disciplina"])

  return df_grade

# Faz o cálculo de quantas disciplinas eletivas e obrigatória o aluno fez por período
def count_carater_por_periodo(df_grade:pd.DataFrame)->pd.DataFrame:

  obrigatoria = df_grade.query("carater == 'O'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_carater_obrigatoria')
  
  facultativa = df_grade.query("carater == 'F'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_carater_facultativa')

  result = pd.merge(obrigatoria, facultativa, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  eletiva = df_grade.query("carater == 'E'").groupby(by=["mat_id", "periodo_que_fez_a_disciplina"])["cod_disciplina"].count().reset_index(name='qnt_carater_eletiva')

  result = pd.merge(result, eletiva, on=['mat_id', 'periodo_que_fez_a_disciplina'], how='outer')

  result['qnt_carater_eletiva'] = result['qnt_carater_eletiva'].fillna(0)
  result['qnt_carater_obrigatoria'] = result['qnt_carater_obrigatoria'].fillna(0)
  result['qnt_carater_facultativa'] = result['qnt_carater_facultativa'].fillna(0)

  df_grade = pd.merge(df_grade, result , how='left', on=['mat_id', "periodo_que_fez_a_disciplina"])

  return df_grade

# Faz o cálculo do coeficiente semestral dos alunos
def semester_coefficient(df:pd.DataFrame) -> pd.DataFrame:
  
  df_not_trancado = df.query("situacao != 'TRANCADO' and situacao != 'CANCELADO'")

  df_aux = df_not_trancado.groupby(by=["mat_id","periodo_que_fez_a_disciplina"])["media_final"].mean().reset_index(name='coeficiente')

  list_index = df_aux.index.to_list()

  for i in list_index:

    row = df_aux.loc[i]
    
    list_alter_index = df.query("mat_id == @row.mat_id and periodo_que_fez_a_disciplina == @row.periodo_que_fez_a_disciplina").index.to_list()
    
    df.loc[list_alter_index, ["coeficiente_semestral"]] = row.coeficiente

  df.loc[df['media_final'] == 0 , 'coeficiente_semestral'] = 0 

  return df


# Faz o calculo do coeficiente acumulado do aluno
def accumulated_coefficient(df:pd.DataFrame) -> pd.DataFrame:
    
  ids_mat = df.mat_id.unique()

  df_coeficientes_acumulados = pd.DataFrame()

  for i in ids_mat:

    coef_acumulado = df.query("mat_id == @i")
    coef_acumulado = coef_acumulado.groupby(by=['mat_id',"periodo_que_fez_a_disciplina"])['coeficiente_semestral'].max().reset_index()
    coef_acumulado = coef_acumulado.set_index("periodo_que_fez_a_disciplina").reset_index()
    coef_acumulado['coeficiente_acumulado'] = np.nan

    for index in coef_acumulado.index : # o index é o período que ele fez a disciplina
    
      if coef_acumulado.loc[index, 'coeficiente_semestral'] == 0 and len(df.query("mat_id == @i and situacao not in ['CANCELADO', 'TRANCADO' ]")) > 0: 
       
        if index == 0:
          coef_acumulado.loc[index , 'coeficiente_acumulado'] = coef_acumulado.loc[ index, 'coeficiente_semestral']
          index_anterior = index
          continue

        else: 
          coef_acumulado.loc[index, 'coeficiente_acumulado'] = coef_acumulado.loc[ index-1, 'coeficiente_acumulado']
          index_anterior = index 
          continue

      if index == 0: 
        coef_acumulado.loc[index, 'coeficiente_acumulado'] = coef_acumulado.loc[index, 'coeficiente_semestral']
        index_anterior = index 
        continue

      if index_anterior == 0 and len(df.query("mat_id == @i and situacao not in ['CANCELADO', 'TRANCADO' ]")) > 0:
        
        coef_acumulado.loc[index , 'coeficiente_acumulado'] = coef_acumulado.loc[ index, 'coeficiente_semestral']
        index_anterior = index
        continue

      coef_acumulado.loc[index, 'coeficiente_acumulado'] = (coef_acumulado.loc[index_anterior, 'coeficiente_acumulado'] + coef_acumulado.loc[index, 'coeficiente_semestral']) / 2
      index_anterior = index 
       

    coef_acumulado = coef_acumulado.reset_index()
    df_coeficientes_acumulados = df_coeficientes_acumulados.append(coef_acumulado)

  df = pd.merge(df, df_coeficientes_acumulados[['periodo_que_fez_a_disciplina','mat_id', 'coeficiente_acumulado']], on=['mat_id', 'periodo_que_fez_a_disciplina'])

  return df
