import pandas as pd
import random
from database import database


#Lista que contiene nombre de universidades según su localidad.
us_states_universities = [
    ("AL", "University of Alabama"),
    ("AK", "University of Alaska"),
    ("AZ", "Arizona State University"),
    ("AR", "University of Arkansas"),
    ("CA", "University of California, Berkeley"),
    ("CO", "University of Colorado Boulder"),
    ("CT", "Yale University"),
    ("DE", "University of Delaware"),
    ("FL", "University of Florida"),
    ("GA", "University of Georgia"),
    ("HI", "University of Hawaii"),
    ("ID", "University of Idaho"),
    ("IL", "University of Illinois Urbana-Champaign"),
    ("IN", "Indiana University Bloomington"),
    ("IA", "University of Iowa"),
    ("KS", "University of Kansas"),
    ("KY", "University of Kentucky"),
    ("LA", "Louisiana State University"),
    ("ME", "University of Maine"),
    ("MD", "University of Maryland"),
    ("MA", "Harvard University"),
    ("MI", "University of Michigan"),
    ("MN", "University of Minnesota"),
    ("MS", "University of Mississippi"),
    ("MO", "University of Missouri"),
    ("MT", "University of Montana"),
    ("NE", "University of Nebraska"),
    ("NV", "University of Nevada, Reno"),
    ("NH", "University of New Hampshire"),
    ("NJ", "Princeton University"),
    ("NM", "University of New Mexico"),
    ("NY", "Columbia University"),
    ("NC", "University of North Carolina at Chapel Hill"),
    ("ND", "University of North Dakota"),
    ("OH", "Ohio State University"),
    ("OK", "University of Oklahoma"),
    ("OR", "University of Oregon"),
    ("PA", "University of Pennsylvania"),
    ("RI", "Brown University"),
    ("SC", "University of South Carolina"),
    ("SD", "University of South Dakota"),
    ("TN", "University of Tennessee"),
    ("TX", "University of Texas at Austin"),
    ("UT", "University of Utah"),
    ("VT", "University of Vermont"),
    ("VA", "University of Virginia"),
    ("WA", "University of Washington"),
    ("WV", "West Virginia University"),
    ("WI", "University of Wisconsin-Madison"),
    ("WY", "University of Wyoming")
]


"""
Función para generar un ID de empleado único de manera aleatoria tomando
en consideración la lista de ID obtenidas del dataset original.
"""
def generate_EmpID(empID_used):
    while True:
        nuevo_id = random.randint(10312, 99999)
        if nuevo_id not in empID_used:
            empID_used.add(nuevo_id)
            return nuevo_id

"""
Función que nos permite asignar un estado geográfico a los empleados.
"""
def assign_states(df):
    if 'State' not in df.columns:
        df['State'] = pd.Series(dtype='object')
    states = list(state_uni_dict.keys())
    missing_state = df['State'].isna()
    df.loc[missing_state, 'State'] = [random.choice(states) for _ in range(missing_state.sum())]
    return df

"""
Función que nos permite asignar una universidad a cada empleado según el estado geográfico.
"""
def assign_universities(df):
    df['University'] = df['State'].map(state_uni_dict)
    return df

"""
Función que nos permite extender el dataset original hasta los 10000 registros con información asignada de manera aleatoria.
"""
def extend_with_random_samples(df, target_count):
    current_len = len(df)
    if current_len >= target_count:
        print(f"No extension needed. Current rows: {current_len}, Target: {target_count}")
        return df

    rows_to_add = target_count - current_len
    original_dtypes = df.dtypes
    unique_vals = {col: df[col].dropna().unique().tolist() for col in df.columns if col != 'EmpID'}

    # Print diagnostics
    #print("Unique values per column (excluding EmpID):")
    for col, values in unique_vals.items():
        #print(f"  {col}: {len(values)} unique")
        pass

    # Track used EmpIDs
    empID_used = set(df['EmpID'].dropna().astype(int).tolist())

    # Generate synthetic rows
    new_rows = []
    for _ in range(rows_to_add):
        row = {
            col: random.choice(values) if values else None
            for col, values in unique_vals.items()
        }
        row['EmpID'] = generate_EmpID(empID_used)
        new_rows.append(row)

    new_df = pd.DataFrame(new_rows)

    # Ensure correct column types
    for col in new_df.columns:
        if col in original_dtypes:
            try:
                new_df[col] = new_df[col].astype(original_dtypes[col])
            except Exception as e:
                print(f"⚠️ Could not cast column {col} to {original_dtypes[col]}: {e}")

    df_extended = pd.concat([df, new_df], ignore_index=True)
    return df_extended

"""
Función principal que que inicia el proceso de expansión de dataset y asignación de datos aleatorios.
"""
def extend_and_assign(df, target_count):
    
    df = extend_with_random_samples(df, target_count)
    df = assign_states(df)
    df = assign_universities(df)
    return df


# Crea diccionario para mapear
state_uni_dict = dict(us_states_universities)
dataset = pd.read_csv("data/HRDataset_v14.csv")
print(f"➤ Total de registros inicial: {len(dataset)}, Número de columnas: {len(dataset.columns)}")

dataset.drop(['Employee_Name','MarriedID','EmpStatusID','FromDiversityJobFairID','Termd',
              'Zip','DOB','CitizenDesc','DeptID','HispanicLatino','DateofHire',
              'DateofTermination','TermReason','EmploymentStatus','Department',
              'ManagerName','ManagerID','RecruitmentSource','EngagementSurvey',
              'EmpSatisfaction','SpecialProjectsCount','LastPerformanceReview_Date',
              'DaysLateLast30','MaritalDesc','RaceDesc','PerformanceScore','Position'
              ], axis=1, inplace=True)

# Se extiende dataset original hasta 10000 registros
dataset = extend_and_assign(dataset, 10000)

print(f"➤ Total de registros extendidos: {len(dataset)}, Número de columnas: {len(dataset.columns)}")

# Se almacena dataset en archivo CSV en carpeta data
dataset.to_csv('data/last_dataset.csv', index=False, quotechar='"', encoding='utf-8-sig')


conexion_db = database()
if(conexion_db.connect_db() == True):
    conexion_db.create_table()
    conexion_db.insert_register()
    dataset.drop(['University', 'State', 'GenderID', 'MaritalStatusID'],axis=1, inplace=True)
    dataset.to_csv('data/last_dataset.csv', index=False, quotechar='"', encoding='utf-8-sig')
else:
    print('ERROR: No se pudo crear la tabla')

print(f"➤ Total de registros final: {len(dataset)}, Número de columnas: {len(dataset.columns)}")
print('INFO: Finalización de creación de datasets.')
