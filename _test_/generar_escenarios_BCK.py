import pandas as pd
import numpy as np
import sys as s
import scipy as sc

periodo_ag=input("Ingrese la frecuencia de agrupación en minutos: ")
periodo_ag_int=int(periodo_ag)*60
frecuencias=[]
frecuencias.append(str(periodo_ag_int)+'s')
num_periodos_atras=input("Ingrese la cantidad de períodos hacia atrás: ")
periodos_atras=[]
periodos_atras=[i for i in range(int(num_periodos_atras))]
max_period=int(num_periodos_atras)-1
combinaciones=4
campos=[]
campos_aux=[]
desc=["V1V2E1E2","V1V2","V1","V1E"]
campos_print=[]
for frecuencia in frecuencias:
	dfv1=pd.read_csv('D:\proyecto_mdd\csv\VARIABLE1.csv', parse_dates=['FECHA'], low_memory=False)
	dfv2=pd.read_csv('D:\proyecto_mdd\csv\VARIABLE2.csv', parse_dates=['FECHA'], low_memory=False)
	dfv1=dfv1[((dfv1.VALOR - dfv1.VALOR.mean()) / dfv1.VALOR.std()).abs() < 2]
	dfv2=dfv2[((dfv2.VALOR - dfv2.VALOR.mean()) / dfv2.VALOR.std()).abs() < 2]
	dfv1=dfv1.dropna(axis=0,how='any')
	dfv2=dfv2.dropna(axis=0,how='any')
	df2v1=dfv1.groupby(['VARIABLE1',pd.Grouper(key='FECHA',freq=frecuencia)])['VALOR'].agg(['mean','count','std'])
	df2v2=dfv2.groupby(['VARIABLE2',pd.Grouper(key='FECHA',freq=frecuencia)])['VALOR'].agg(['sum','count','std'])
	df2v1['ERROR']=(df2v1['std']/(np.sqrt(df2v1['count'])))
	df2v2['ERROR']=(df2v2['std']/(np.sqrt(df2v2['count'])))
	df2v1=df2v1.drop('count',1)
	df2v2=df2v2.drop('count',1)
	df2v1=df2v1.drop('std',1)
	df2v2=df2v2.drop('std',1)
	df2v1.rename(columns={'mean':'MEDIA'},inplace=True)
	df2v2.rename(columns={'sum':'MEDIA'},inplace=True)
	df2v1=df2v1[((df2v1.MEDIA - df2v1.MEDIA.mean()) / df2v1.MEDIA.std()).abs() < 2]
	df2v2=df2v2[((df2v2.MEDIA - df2v2.MEDIA.mean()) / df2v2.MEDIA.std()).abs() < 2]
	df2v1=df2v1.reset_index()
	df2v2=df2v2.reset_index()
	df2v1['indice']=df2v1.index
	df2v2['indice']=df2v2.index
	cols=['indice','FECHA','MEDIA','ERROR']
	df2v1=df2v1[cols]
	df2v2=df2v2[cols]	
	#df2v1.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\V1'+frecuencia+'.csv',index=False)
	#df2v2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\V2'+frecuencia+'.csv',index=False)
	stage=[i for i in periodos_atras]
	tmp_dfv1=[j for j in periodos_atras]
	tmp_dfv2=[k for k in periodos_atras]
	frecuencia_int=int(frecuencia.replace("s",""))
	frecuencia_int=frecuencia_int/60
	frec_escenario=str(frecuencia_int)+"min"
	
	
	for periodo_atras in periodos_atras:
		cont=int(num_periodos_atras)-periodo_atras
		tmp_dfv1[periodo_atras]=df2v1
		tmp_dfv2[periodo_atras]=df2v2	
		#stage[periodo_atras]=pd.merge(df2v2,df2v1, on='indice', how='left')	
		stage[periodo_atras]=pd.merge(df2v1,df2v2, on='indice', how='left')			
		tmp_dfv1[periodo_atras]=tmp_dfv1[periodo_atras][tmp_dfv1[periodo_atras].indice>periodo_atras]
		tmp_dfv2[periodo_atras]=tmp_dfv2[periodo_atras][tmp_dfv2[periodo_atras].indice>periodo_atras]
		tmp_dfv1[periodo_atras]=tmp_dfv1[periodo_atras].reset_index()
		tmp_dfv2[periodo_atras]=tmp_dfv2[periodo_atras].reset_index()
		tmp_dfv1[periodo_atras]['indice']=tmp_dfv1[periodo_atras].index
		tmp_dfv2[periodo_atras]['indice']=tmp_dfv2[periodo_atras].index
		cols=['indice','FECHA','MEDIA','ERROR']
		tmp_dfv1[periodo_atras]=tmp_dfv1[periodo_atras][cols]
		tmp_dfv2[periodo_atras]=tmp_dfv2[periodo_atras][cols]
		#tmp_dfv1[periodo_atras].rename(columns={'FECHA':'V1_FECHA-'+str(cont),'MEDIA':'V1_MEDIA-'+str(cont),'ERROR':'V1_ERROR-'+str(cont)},inplace=True)
		#tmp_dfv2[periodo_atras].rename(columns={'FECHA':'V2_FECHA-'+str(cont),'MEDIA':'V2_MEDIA-'+str(cont),'ERROR':'V2_ERROR-'+str(cont)},inplace=True)
		#tmp_dfv1[periodo_atras].to_csv('D:\proyecto_mdd\csv\ESCENARIOS\V1'+frecuencia+'-'+str(periodo_atras)+'.csv',index=False)
		#tmp_dfv2[periodo_atras].to_csv('D:\proyecto_mdd\csv\ESCENARIOS\V2'+frecuencia+'-'+str(periodo_atras)+'.csv',index=False)				
	for periodo_atras in reversed(periodos_atras):
		if periodo_atras==int(num_periodos_atras)-1:
			stage[periodo_atras]=pd.merge(stage[periodo_atras],tmp_dfv1[max_period-periodo_atras], on='indice', how='left', suffixes=['_l', '_r'])
			stage[periodo_atras].rename(columns={'FECHA_y':'V2_FECHA-1_','MEDIA_y':'V2_MEDIA-1_','ERROR_y':'V2_ERROR-1_'},inplace=True)
			stage[periodo_atras].rename(columns={'FECHA_x':'V1_FECHA-1_','MEDIA_x':'V1_MEDIA-1_','ERROR_x':'V1_ERROR-1_'},inplace=True)
			for comb in range(combinaciones):
				#print(comb)	
				if comb==0:
					campos=[i for i in range(len(stage[periodo_atras].columns)-1)]
					campos.pop(0)
					for i in range (1,len(campos)-2,3):
						campos.remove(i)					
					
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage[periodo_atras].iloc[:,pd.eval(campos_print)]
					#stage2=stage2[(np.abs(sc.zscore(stage2)) < 3).all(axis=1)]
					#stage2=stage2[stage2.apply(lambda x: np.abs(x - x.mean()) / x.std() < 2).all(axis=1)]
					#stage2=stage2[((stage2.MEDIA - stage2.MEDIA.mean()) / stage2.MEDIA.std()).abs() < 2]
					#print(stage2)
					#stage2=stage2[((stage2.MEDIA - stage2.MEDIA.mean()) / stage2.MEDIA.std()).abs() < 2]
					stage2=stage2.dropna(axis=0,how='any')
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]					
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					#print (campos)
				if comb==1:
					for i in range (3,campos[-1]-1,3):
						campos.remove(i)
						campos_print=campos[:]
						campos_print.insert(0,campos[-2])
						campos_print.pop(-2)
						stage2=stage[periodo_atras].iloc[:,pd.eval(campos_print)]
						stage2=stage2.dropna(axis=0,how='any')
						filas=len(stage2.axes[0])
						#print(filas)
						filas_entr=int(filas*0.7)
						#print(filas_entr)
						filas_test=filas-filas_entr
						#print(filas_test)
						stage_entr=stage2.iloc[:filas_entr]
						stage_test=stage2.iloc[filas_entr+1:]
						#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
						stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
						stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==2:
					campos.pop(1)		
					#campos.pop(-3)
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage[periodo_atras].iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==3:
					campos_aux.append(campos[0])
					campos_aux.append(campos[0]+1)
					campos.pop(0)	
					campos.insert(0,campos_aux[1])
					campos.insert(0,campos_aux[0])
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage[periodo_atras].iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print(campos)						
			#stage[periodo_atras].to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'.csv',index=False)		
			#print(len(stage[periodo_atras].columns))
			stage[periodo_atras]=pd.merge(stage[periodo_atras],tmp_dfv2[max_period-periodo_atras], on='indice', how='left')
			
			stage_tmp=stage[periodo_atras]
			max=max_period-periodo_atras+2
			fact=2*(max_period-periodo_atras)
			sup=max*(4)+fact
			#campos.extend([0,sup-1,sup])			
			#print(campos)
			
			#for comb in range(combinaciones):
				#print(comb)	
				#max=max_period-periodo_atras+2
				#print(max)
		elif periodo_atras== 0:
			for i in reversed (range(max_period-periodo_atras+1)):			
				stage_tmp.columns=stage_tmp.columns.str.replace('-'+str(i)+'_','-'+str(i+1)+'_')
			stage_tmp=pd.merge(stage_tmp,tmp_dfv1[max_period-periodo_atras], on='indice', how='left')
			stage_tmp.rename(columns={'FECHA_y':'V2_FECHA-1_','MEDIA_y':'V2_MEDIA-1_','ERROR_y':'V2_ERROR-1_'},inplace=True)
			stage_tmp.rename(columns={'FECHA_x':'V1_FECHA-1_','MEDIA_x':'V1_MEDIA-1_','ERROR_x':'V1_ERROR-1_'},inplace=True)
			#stage_tmp.rename(columns={'FECHA_x':'V1_FECHA-'+(num_periodos_atras),'MEDIA_x':'V1_MEDIA-'+(num_periodos_atras),'ERROR_x':'V1_ERROR-'+(num_periodos_atras)},inplace=True)
			#stage_tmp.rename(columns={'FECHA_y':'V2_FECHA-'+(num_periodos_atras),'MEDIA_y':'V2_MEDIA-'+(num_periodos_atras),'ERROR_y':'V2_ERROR-'+(num_periodos_atras)},inplace=True)
			for comb in range(combinaciones):
				#print(comb)	
				if comb==0:
					campos=[i for i in range(len(stage_tmp.columns)-1)]
					campos.pop(0)
					for i in range (1,len(campos)-2,3):
						campos.remove(i)
						campos_print=campos[:]
						campos_print.insert(0,campos[-2])
						campos_print.pop(-2)
						stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
						stage2=stage2.dropna(axis=0,how='any')
						#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
						filas=len(stage2.axes[0])
						#print(filas)
						filas_entr=int(filas*0.7)
						#print(filas_entr)
						filas_test=filas-filas_entr
						#print(filas_test)
						stage_entr=stage2.iloc[:filas_entr]
						stage_test=stage2.iloc[filas_entr+1:]
						stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
						stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==1:
					for i in range (3,campos[-1]-1,3):
						campos.remove(i)
						campos_print=campos[:]
						campos_print.insert(0,campos[-2])
						campos_print.pop(-2)
						stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
						stage2=stage2.dropna(axis=0,how='any')
						filas=len(stage2.axes[0])
						#print(filas)
						filas_entr=int(filas*0.7)
						#print(filas_entr)
						filas_test=filas-filas_entr
						#print(filas_test)
						stage_entr=stage2.iloc[:filas_entr]
						stage_test=stage2.iloc[filas_entr+1:]
						#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
						stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
						stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==2:
					campos.pop(1)
					campos.pop(-3)
					for i in range (1,(max_period-periodo_atras),1):
						campos.pop(-i-3)
						#print (-i)		
					#campos.pop(-4)
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras##+1))+'_'+desc[comb]+'.csv',index=False)			
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==3:
					campos.insert(1,campos[0]+1)
					for i in range (1,(max_period-periodo_atras+1), 1):
						campos.insert(i*2+1,campos[i*2]+1)
						#print (i)
				
					#campos_aux.append(campos[0])
					#campos_aux.append(campos[0]+1)
					#campos.pop(0)	
					#campos.insert(0,campos_aux[1])
					#campos.insert(0,campos_aux[0])
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print(campos)				
			
		else:
			for i in reversed (range(max_period-periodo_atras+1)):			
				stage_tmp.columns=stage_tmp.columns.str.replace('-'+str(i)+'_','-'+str(i+1)+'_')
			stage_tmp=pd.merge(stage_tmp,tmp_dfv1[max_period-periodo_atras], on='indice', how='left')
			#stage_tmp.rename(columns={'FECHA_x':'V2_FECHA-'+str((max_period-periodo_atras+1)),'MEDIA_x':'V2_MEDIA-'+str((max_period-periodo_atras+1)),'ERROR_x':'V2_ERROR-'+str((max_period-periodo_atras+1))},inplace=True)
			#stage_tmp.rename(columns={'FECHA_y':'V1_FECHA-'+str((max_period-periodo_atras+1)),'MEDIA_y':'V1_MEDIA-'+str((max_period-periodo_atras+1)),'ERROR_y':'V1_ERROR-'+str((max_period-periodo_atras+1))},inplace=True)
			stage_tmp.rename(columns={'FECHA_y':'V2_FECHA-1_','MEDIA_y':'V2_MEDIA-1_','ERROR_y':'V2_ERROR-1_'},inplace=True)
			stage_tmp.rename(columns={'FECHA_x':'V1_FECHA-1_','MEDIA_x':'V1_MEDIA-1_','ERROR_x':'V1_ERROR-1_'},inplace=True)
			for comb in range(combinaciones):
				#print(comb)	
				if comb==0:
					campos=[i for i in range(len(stage_tmp.columns)-1)]
					campos.pop(0)
					for i in range (1,len(campos)-2,3):
						campos.remove(i)
						campos_print=campos[:]
						campos_print.insert(0,campos[-2])
						campos_print.pop(-2)
						stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
						stage2=stage2.dropna(axis=0,how='any')
						#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
						filas=len(stage2.axes[0])
						#print(filas)
						filas_entr=int(filas*0.7)
						#print(filas_entr)
						filas_test=filas-filas_entr
						#print(filas_test)
						stage_entr=stage2.iloc[:filas_entr]
						stage_test=stage2.iloc[filas_entr+1:]
						stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
						stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (stage2)
					#print (campos)
				if comb==1:
					for i in range (3,campos[-1]-1,3):
						campos.remove(i)
						campos_print=campos[:]
						campos_print.insert(0,campos[-2])
						campos_print.pop(-2)
						stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
						stage2=stage2.dropna(axis=0,how='any')
						filas=len(stage2.axes[0])
						#print(filas)
						filas_entr=int(filas*0.7)
						#print(filas_entr)
						filas_test=filas-filas_entr
						#print(filas_test)
						stage_entr=stage2.iloc[:filas_entr]
						stage_test=stage2.iloc[filas_entr+1:]
						#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
						stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
						stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==2:
					campos.pop(1)
					campos.pop(-3)
					for i in range (1,(max_period-periodo_atras),1):
						campos.pop(-i-3)
						#print (-i)		
					#campos.pop(-4)
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print (campos)
				if comb==3:
					campos.insert(1,campos[0]+1)
					for i in range (1,(max_period-periodo_atras+1), 1):
						campos.insert(i*2+1,campos[i*2]+1)
						#print (i)


					
					#campos_aux.append(campos[0])
					#campos_aux.append(campos[0]+1)
					#campos.pop(0)	
					#campos.insert(0,campos_aux[1])
					#campos.insert(0,campos_aux[0])
					campos_print=campos[:]
					campos_print.insert(0,campos[-2])
					campos_print.pop(-2)
					stage2=stage_tmp.iloc[:,pd.eval(campos_print)]
					stage2=stage2.dropna(axis=0,how='any')
					#stage2.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'.csv',index=False)			
					filas=len(stage2.axes[0])
					#print(filas)
					filas_entr=int(filas*0.7)
					#print(filas_entr)
					filas_test=filas-filas_entr
					#print(filas_test)
					stage_entr=stage2.iloc[:filas_entr]
					stage_test=stage2.iloc[filas_entr+1:]
					stage_entr.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_entr.csv',index=False)
					stage_test.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario_'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb]+'_test.csv',index=False)
					print('Escenario de entrenamiento generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])									
					print('Escenario de test generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print('Escenario generado: '+frec_escenario+'-'+str((max_period-periodo_atras+1))+'_'+desc[comb])
					#print(campos)				
			#stage_tmp.to_csv('D:\proyecto_mdd\csv\ESCENARIOS\escenario'+frec_escenario+'-'+str((max_period-periodo_atras+1))+'.csv',index=False)			
			#print(len(stage_tmp.columns))
			stage_tmp=pd.merge(stage_tmp,tmp_dfv2[max_period-periodo_atras], on='indice', how='left')
			#print (periodo_atras)
			#print (num_periodos_atras)
			max=max_period-periodo_atras+2
			fact=2*(max_period-periodo_atras)
			sup=max*(4)+fact
			#print(sup)			