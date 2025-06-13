# %% [markdown]
# # Python Fluent Interface / Method Chaining
# Ich wollte einmal hier festhalten wie man sogenannte "Fluent Interfaces" in Python schreibt, mit einem kleinen technischen DeepDive zu Python am Ende.
# 
# Hintergrund war, in Fabric, kann man nicht ohne weiteres Tabellen bearbeiten, ärgerlich vor allem wenn man "Configurations-Tabellen" hat die man oft anpassen oder erweitern möchte. Ein kleines "Workbench"-Python Notebook ist da quasi die schnellste Methode.
# 
# Nach dem ich zum fünften mal so etwas getippt habe:

# %%
from pyspark.sql.functions import when, col
df = spark.sql("SELECT * FROM Foo.dbo.data_processing_information")
updated_df = df.withColumn( "primaryKeyColumns", when(col("Fileprefix") == "SL_GP_Basis", "Foo;bar") .otherwise(col("primaryKeyColumns")))
display(updated_df)

# %% [markdown]
# Habe ich gedacht es wird Zeit für eine Helfer-Funktion:

# %%
# mocks
from typing import Any

class Mock:
    def __init__(self, name: str, return_extra: Any = None):
        """Initializes the mock object."""
        self.name = name
        self.return_extra = return_extra
    def __call__(self, *args, **kwargs) -> Any:
        """This makes instances of the class callable, like a function."""
        print(f"{self.name} <- {args}, {kwargs}")
        return self.return_extra
    def __repr__(self) -> str:
        """Provides the custom, developer-friendly representation."""
        # This is what you see when you just type the object's name
        # in a notebook cell or use print().
        return f"<{self.name}-mock>"
    
class MSpark:    
    class MDf:
        withColumn = Mock("withColumn")
    sql = Mock("spark.sql", MDf())
    table = Mock("spark.table", MDf())
spark = MSpark()
class MOther:
    otherwise = Mock("otherwise")
when = Mock("when", MOther())
col= Mock("col")
lit = Mock("lit")

# %%
def updateRowField(table, column, whereCol, whereVal, value):
    df = spark.sql(f"SELECT * FROM {table}")
    updated_df = df.withColumn( column, when(col(whereCol) == whereVal, value) .otherwise(col(column)))
    display(updated_df)
    
updateRowField(table="Foo.dbo.data_processing_information", column="primaryKeyColumns",whereCol="Fileprefix", whereVal="SL_GP_Basis",value="Foo;bar")

# %% [markdown]
# Was aber immernoch nicht besonders Userfreundlich ist.
# 
# # Fluent Interfaces
# Ich hätte gerne, wie es Assertion-frameworks oder DataFrames machen, so etwas geschrieben: `udpated_df = doSet("LH_SAP_DATA.dbo.data_processing_information").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")`
# 
# Dafür bietet sich eine Helper-Klasse an.

# %%
class SparkTableUpdater:
    """
    A fluent interface helper to perform a conditional update on a Spark DataFrame.
    """
    def __init__(self, table_name: str):
        # Initial state
        self.spark = spark
        self._table_name = table_name
        self._df = None
        
        # State to be built by the chain
        self._filter_column = None
        self._filter_value = None
        self._update_column = None

    def rowWhereColumn(self, column_name: str):
        """Specifies the column for the WHERE clause."""
        print(f"STATE: Setting filter column to '{column_name}'")
        self._filter_column = column_name
        return self # <-- The magic! Return the object itself.

    def equals(self, value: any):
        """Specifies the value to check for equality in the WHERE clause."""
        print(f"STATE: Setting filter value to '{value}'")
        self._filter_value = value
        return self # <-- Return the object to allow further chaining.

    def changeColumn(self, column_name: str):
        """Specifies the column to be updated."""
        print(f"STATE: Setting column to update to '{column_name}'")
        self._update_column = column_name
        return self # <-- Again, return self.

    def to(self, new_value: any):
        """
        TERMINATING METHOD: Executes the update and returns the final DataFrame.
        This is the end of the chain.
        """
        print("ACTION: Executing the update...")
        # --- Validation ---
        if not all([self._filter_column, self._update_column]):
            raise ValueError("Incomplete chain. You must call rowWhereColumn(), equals(), and changeColumn() before .to()")
        
        # --- Execution ---
        # 1. Load the initial DataFrame
        df = self.spark.table(self._table_name)

        # 2. Apply the logic using standard PySpark functions
        updated_df = df.withColumn(
            self._update_column,
            when(col(self._filter_column) == self._filter_value, lit(new_value))
            .otherwise(col(self._update_column))
        )
        
        # 3. This method returns the final result, not 'self'
        return updated_df

# Optional: A factory function to make the start of the chain look cleaner
def doSet(table_name: str):
    """Factory function to initialize the SparkTableUpdater."""
    return SparkTableUpdater(table_name)


# %% [markdown]
# Dann funktioniert:

# %%
doSet("LH_SAP_DATA.dbo.data_processing_information").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")
doSet("LH_SAP_DATA.dbo.data_processing_information").equals('SL_GP_Basis').rowWhereColumn('Fileprefix').changeColumn('primaryKeyColumns').to("Foo;bar")
# Fehler output
doSet("LH_SAP_DATA.dbo.data_processing_information").equals('SL_GP_Basis').to("Foo;bar")

# %% [markdown]
# Der Nachteil ist das `doSet("LH_SAP_DATA.dbo.data_processing_information").equals('SL_GP_Basis').rowWhereColumn('Fileprefix').changeColumn('primaryKeyColumns').to("Foo;bar")` genauso funktioniert aber nicht mehr lesbar ist. Um die Reihenfolgen zu erzwingen könnte man interne Klassen verwenden.
# 
# ## State-Machine Approach

# %%
class SparkTableUpdater2:
    """
    A fluent, state-safe interface to perform a conditional update on a Spark DataFrame.
    Uses nested classes to enforce method order and encapsulate state.
    """

    # --- Step 1: The Entry Point (Outer Class) ---
    def __init__(self, table_name: str):
        self._spark = spark
        self._table_name = table_name
        
        # These will be set by the inner classes
        self._filter_column = None
        self._operator = None
        self._filter_value = None
        self._update_column = None

    def rowWhereColumn(self, column_name: str):
        """Specifies the column for the WHERE clause."""
        print(f"STATE: Setting filter column to '{column_name}'")
        self._filter_column = column_name
        return self._ConditionBuilder(self) # Pass a reference to the outer instance

    # --- Step 2: Nested Class for Building the Condition ---
    class _ConditionBuilder:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            # Store a reference to the outer SparkTableUpdater instance
            self.outer = updater_instance

        def equals(self, value: any):
            """Specifies the value to check for equality."""
            print(f"STATE: Setting filter operator to '==' and value to '{value}'")
            self.outer._operator = "=="
            self.outer._filter_value = value
            return self.outer._TargetColumnSelector(self.outer)

        def isGreaterThan(self, value: any):
            """Specifies the value to check for greater than."""
            print(f"STATE: Setting filter operator to '>' and value to '{value}'")
            self.outer._operator = ">"
            self.outer._filter_value = value
            return self.outer._TargetColumnSelector(self.outer)

    # --- Step 3: Nested Class for Selecting the Target Column ---
    class _TargetColumnSelector:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            self.outer = updater_instance
        
        def changeColumn(self, column_name: str):
            """Specifies the column to be updated."""
            print(f"STATE: Setting column to update to '{column_name}'")
            self.outer._update_column = column_name
            return self.outer._UpdateExecutor(self.outer)

    # --- Step 4: Nested Class for the Final Execution ---
    class _UpdateExecutor:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            self.outer = updater_instance

        def to(self, new_value: any):
            """TERMINATING METHOD: Executes the update and returns the final DataFrame."""
            print("ACTION: Executing the update...")
            
            # Access attributes directly from the outer instance
            df = self.outer._spark.table(self.outer._table_name)
            
            condition = None
            if self.outer._operator == "==":
                condition = (col(self.outer._filter_column) == self.outer._filter_value)
            elif self.outer._operator == ">":
                 condition = (col(self.outer._filter_column) > self.outer._filter_value)
            else:
                raise NotImplementedError(f"Operator '{self.outer._operator}' not supported.")

            updated_df = df.withColumn(
                self.outer._update_column,
                when(condition, lit(new_value))
                .otherwise(col(self.outer._update_column))
            )
            return updated_df
        
def doSet2(table_name: str):
    """Factory function to initialize the SparkTableUpdater."""
    return SparkTableUpdater2(table_name)

# %% [markdown]
# Jetzt kann der Entwickler die Methoden nur noch in der vorgegebenen Reihenfolge verwenden. Außerdem meldet die IDE die verfügbaren Methoden via auto-complete und bemängelt falsches Verwenden direkt im Editor.
# 
# ### Side-Note
# 
# In den Unterklassen von `SparkTableUpdater2` zum Zeitpunkt wenn die Methoden-Signaturen initialisiert werden, exisitiert der Typ "SparkTableUpdater2" noch nicht. Deswegen muss der Type hier mit Anführungszeichen `__init__(self, updater_instance: "SparkTableUpdater2")` angegeben werden. Dies meldet dem Language-Server das dieser Type erst "nachträglich" verfügbar ist.

# %%
doSet2("LH_SAP_DATA.dbo.data_processing_information").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")
# Fehler output
# Cannot access attribute "equals" for class "SparkTableUpdater2"
#  Attribute "equals" is unknownPylancereportAttributeAccessIssue
# doSet2("LH_SAP_DATA.dbo.data_processing_information").~~equals('SL_GP_Basis')~~.rowWhereColumn('Fileprefix').changeColumn('primaryKeyColumns').to("Foo;bar")
f = open("")
f.__class__.__base__

# %% [markdown]
# # Zuviel Boiler-Plate? Python unter der Haube.
# Das ist aber alles recht viel Schreibaufwand für "nur" eine Hilfs-Funktion. Aber hier kann man eine Eigenschaft von Python als interpretierte "Duck-Typed" Sprache zu nutzen machen.
# 
# ### The "Magic" Method: __getattr__
# In Python, wenn man versucht auf ein Attribute `my_thing.something` zuzugreifen, schaut Python an mehere stellen. Als erstes im `my_thing.__dict__` Dictionary der Instanz, dann ihrer Klasse `my_thing.__class__`, Super-Klasse `my_thing.__class__.__base__`. Wenn all diese Orte nichts liefern, dann ruft Python als letzter Versuch `my_thing.__getattr__(self, name)` auf, wenn es auf der Instanz existiert.
# 
# Diese Methode kann man "hijacken" um die Fluent API in einer einzigen Klasse zu implementieren.

# %%
from functools import partial

class DynamicUpdater:
    # Define the valid methods for each state of our fluent API
    _VALID_METHODS = {
        'initial': {'rowWhereColumn'},
        'condition_set': {'equals', 'isGreaterThan'},
        'operator_set': {'changeColumn'},
        'target_set': {'to'}
    }

    def __init__(self, table_name: str):
        self._spark = spark
        self._table_name = table_name
        
        # The current state of the builder
        self._state = 'initial'
        
        # Placeholders for the query parts
        self._query_parts = {}

    def __getattr__(self, name):
        # This magic method is called ONLY when an attribute is not found
        
        # 1. Is the called method valid for the current state?
        if name in self._VALID_METHODS.get(self._state, set()):
            # 2. Yes. Return a handler function that will execute the logic.
            #    We use `partial` to pre-fill the method name for our dispatcher.
            return partial(self._dispatcher, name)
        
        # 3. No. The method call is invalid for the current state.
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}' in state '{self._state}'. "
            f"Valid methods: {self._VALID_METHODS.get(self._state, 'None')}"
        )

    def _dispatcher(self, method_name, *args, **kwargs):
        """A central place to handle the logic for each valid method."""
        
        # --- Logic for 'rowWhereColumn' ---
        if method_name == 'rowWhereColumn':
            self._query_parts['filter_col'] = args[0]
            self._state = 'condition_set' # Transition to the next state

        # --- Logic for operators ---
        elif method_name in ('equals', 'isGreaterThan'):
            operator_map = {'equals': '==', 'isGreaterThan': '>'}
            self._query_parts['operator'] = operator_map[method_name]
            self._query_parts['filter_val'] = args[0]
            self._state = 'operator_set' # Transition to the next state
        
        # --- Logic for 'changeColumn' ---
        elif method_name == 'changeColumn':
            self._query_parts['update_col'] = args[0]
            self._state = 'target_set' # Transition to the next state

        # --- Terminating logic for 'to' ---
        elif method_name == 'to':
            # This is the final method, it does not return self
            return self._execute_update(args[0])

        # Return self to allow method chaining
        return self

    def _execute_update(self, new_value):
        """The final execution logic, pulled from our previous example."""
        qp = self._query_parts
        df = self._spark.table(self._table_name)

        condition = None
        if qp['operator'] == '==':
            condition = (col(qp['filter_col']) == qp['filter_val'])
        elif qp['operator'] == '>':
            condition = (col(qp['filter_col']) > qp['filter_val'])
        
        return df.withColumn(
            qp['update_col'],
            when(condition, lit(new_value)).otherwise(col(qp['update_col']))
        )

# Factory function remains the same
def doDynamicSet(table_name: str) -> DynamicUpdater:
    return DynamicUpdater(table_name)


# %%
doDynamicSet("LH_SAP_DATA.dbo.data_processing_information").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")
doDynamicSet('Fehler').foo('stuff')

# %% [markdown]
# Der Nachteil ist, dass die verfügbaren Methoden auf einen dynamischen Laufzeit-Zustand beruhen. Daher verliert man die Hilfe des Linters/IDE. Also in diesem Fall eher ein Akademisches Beispiel als eine empfohlene Art und Weiße das Kontext-Problem zu lösen.
# 
# ### Mehr Magie: Class Factories
# Instanzen werden von Python über die `__new__` Methode an der Klasse selbst erzeugt. Diese kann man überschreiben wenn man die Instanzierung der Objekte manipulieren möchte.

# %%
from typing import Type, TypeVar, cast


A = TypeVar("A")

class Agent:
    def run(self, text):
        print(text)

class APrinterAgent(Agent):
    def run(self, text):
        super().run(f"A{text}A")

class BPrinterAgent(Agent):
    def run(self, text):
        super().run(f"B{text}B")
        
class MultiShotAgent[A: Agent]:
    def __new__(
        cls,
        agent_class: Type[A], 
        repeat: int = 3,
        *args,
        **kwargs
    ) -> A: # The return type is an instance of the provided agent class
        
        def multi_run(self, text):
            text = "".join([text]* self.repeat)
            # This super() will find the 'run' method on the parent (e.g., BPrinterAgent)
            super(dynamic_multi_agent, self).run(text)

        dynamic_multi_agent = type(
            f"MultiShot{agent_class.__name__}",
            (agent_class,),  # Inherit from the provided agent class
            {
                "run": multi_run, # Use the new method we just defined
                "repeat": repeat  # Add repeat as a class attribute
            }
        )

        #    We cast it to 'A' which is the TypeVar for the agent_class
        instance = cast(A, dynamic_multi_agent(*args, **kwargs))
        return instance


# %%

APrinterAgent().run("foo")
bp = BPrinterAgent()
bp.run("foo")
mbp: BPrinterAgent
mbp = MultiShotAgent(BPrinterAgent,5)
mbp.run("foo")


# %% [markdown]
# Diese Fluent Api könnte man also auch so definieren:

# %%
# Step 1: The Blueprint. This data structure IS our API definition.
API_BLUEPRINT = {
    'initial': {
        'methods': {
            # method_name: (state_key_to_set, next_state_name)
            'rowWhereColumn': ('filter_col', 'condition_builder'),
        }
    },
    'condition_builder': {
        'methods': {
            'equals': ('filter_val', 'target_selector'),
            'isGreaterThan': ('filter_val', 'target_selector'),
        },
        # We can add extra logic, like mapping method names to operators
        'operator_map': {'equals': '==', 'isGreaterThan': '>'}
    },
    'target_selector': {
        'methods': {
            'changeColumn': ('update_col', 'executor'),
        }
    },
    'executor': {
        'methods': {
            # The terminator is special, it has no next state.
            'to': (None, None),
        }
    }
}

def _execute_final_update(query_parts, spark_session, table_name):
    """The final action. This is normal, non-meta code."""
    df = spark_session.table(table_name)
    qp = query_parts
    
    condition = None
    if qp['operator'] == '==':
        condition = (col(qp['filter_col']) == qp['filter_val'])
    elif qp['operator'] == '>':
        condition = (col(qp['filter_col']) > qp['filter_val'])

    return df.withColumn(
        qp['update_col'],
        when(condition, lit(qp['new_value'])).otherwise(col(qp['update_col']))
    )


class FluentAPI:
    def __new__(cls, state_name: str):
        """Dynamically creates a class type for a given state in the API chain."""
        
        class_name = f"DynamicState_{state_name.title().replace('_','')}"
        state_blueprint = API_BLUEPRINT[state_name]
        
        # This is the handler that will become a method on our new class.
        def _make_method_handler(method_name, key_to_set, next_state):
            # This is a closure. It captures the variables from its defining scope.
            def handler(self, value):
                # 1. Update the shared query parts dictionary
                self.query_parts[key_to_set] = value
                
                # Special handling for operators
                if 'operator_map' in state_blueprint:
                    self.query_parts['operator'] = state_blueprint['operator_map'][method_name]

                # 2. Create the class for the NEXT state in the chain
                NextStateClass = cls(next_state)
                
                # 3. Return an INSTANCE of the new class, passing the state along
                return NextStateClass(self.query_parts, self.spark, self.table_name)
            return handler

        # The handler for the terminating 'to' method is different
        def _terminating_handler(self, value):
            self.query_parts['new_value'] = value
            # It doesn't create a new class, it executes the final logic.
            return _execute_final_update(self.query_parts, self.spark, self.table_name)

        # --- Assemble the new class ---
        class_attributes = {}
        
        # The __init__ for every dynamic class will be the same.
        def __init__(self, query_parts, spark, table_name):
            self.query_parts = query_parts
            self.spark = spark
            self.table_name = table_name
        class_attributes['__init__'] = __init__

        # Dynamically create and attach the methods for this state
        for method_name, (key_to_set, next_state) in state_blueprint['methods'].items():
            if method_name == 'to':
                class_attributes[method_name] = _terminating_handler
            else:
                class_attributes[method_name] = _make_method_handler(method_name, key_to_set, next_state)
                
        # The magic: type(ClassName, (Bases,), {Attributes}) creates a new class object
        return type(class_name, (object,), class_attributes)


# Step 3: The User-Facing Factory Function
def doSetWithFactory( table_name: str):
    """The entry point that kicks off the dynamic class generation chain."""
    # Create the initial class for the 'initial' state
    InitialStateClass = FluentAPI('initial')
    
    # Return an instance of it, starting with an empty state dictionary
    return InitialStateClass(query_parts={}, spark=spark, table_name=table_name)

# %%
doSetWithFactory("LH_SAP_DATA.dbo.data_processing_information").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")


