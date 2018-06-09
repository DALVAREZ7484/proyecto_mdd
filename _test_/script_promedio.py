import pd as pandas;
import np as numpy;
df=pd.read_csv('D:\proyecto_mdd\csv\ARCHIVO.csv', index_col=0, parse_dates=True);
periodo=df.index.to_period("M");
df2=df.groupby([ periodo, df.index.time]).avg();
df2.to_csv('promedio_cont_1.csv');