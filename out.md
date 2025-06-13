
## Fluent Interfaces in Python: Vom einfachen Helfer zur robusten State-Machine

In diesem Artikel tauchen wir tief in das Thema "Fluent Interfaces" (auch als Methodenverkettung bekannt) in Python ein. Wir beginnen mit einem alltäglichen Problem aus der Praxis und entwickeln schrittweise verschiedene Lösungen, von einfachen Helfern bis hin zu fortgeschrittenen, dynamischen Ansätzen.

### Das Ausgangsproblem: Manuelle Updates in Fabric

Wer mit Spark in Microsoft Fabric arbeitet, kennt das Problem: Konfigurationstabellen, die oft angepasst werden müssen, lassen sich nicht einfach wie in einer SQL-Datenbank per `UPDATE`-Statement bearbeiten. Der schnellste Weg ist oft ein kleines Python-Notebook.

Nachdem man aber zum fünften Mal folgenden, leicht umständlichen PySpark-Code geschrieben hat, wünscht man sich eine elegantere Lösung:

```python
from pyspark.sql.functions import when, col

# Der wiederkehrende, manuelle Code
df = spark.sql("SELECT * FROM Foo.dbo.data_processing_information")

updated_df = df.withColumn(
    "primaryKeyColumns",
    when(col("Fileprefix") == "SL_GP_Basis", "Foo;bar")
    .otherwise(col("primaryKeyColumns"))
)

display(updated_df)
```

Eine erste Verbesserung ist eine simple Helfer-Funktion, aber das ist immer noch nicht wirklich intuitiv oder "lesbar":

```python
def updateRowField(table, column, whereCol, whereVal, value):
    df = spark.sql(f"SELECT * FROM {table}")
    updated_df = df.withColumn(
        column,
        when(col(whereCol) == whereVal, value)
        .otherwise(col(column))
    )
    display(updated_df)

# Aufruf
updateRowField(
    table="Foo.dbo.data_processing_information",
    column="primaryKeyColumns",
    whereCol="Fileprefix",
    whereVal="SL_GP_Basis",
    value="Foo;bar"
)
```

### Das Ziel: Eine ausdrucksstarke Fluent API

Viel schöner wäre eine Syntax, wie man sie von DataFrame-APIs oder Assertion-Frameworks kennt. Eine Code-Zeile, die sich fast wie ein englischer Satz liest:

```python
# Das ist unser Ziel
updater.table("...") \
       .rowWhereColumn('Fileprefix').equals('SL_GP_Basis') \
       .changeColumn('primaryKeyColumns').to("Foo;bar")
```

Um das zu erreichen, bauen wir eine Helfer-Klasse.

### Ansatz 1: Der einfache Builder mit `return self`

Die grundlegendste Technik für eine Fluent API ist, dass jede Methode, die die Kette fortsetzen soll, die eigene Instanz (`self`) zurückgibt. Eine finale Methode ("Terminator") führt dann die Aktion aus und gibt das Endergebnis zurück.

```python
class SparkTableUpdater:
    """
    Ein einfacher Fluent-Interface-Helfer, der `return self` nutzt.
    """
    def __init__(self, table_name: str):
        # Initialer Zustand
        self._table_name = table_name
        
        # Zustand, der durch die Methodenverkettung aufgebaut wird
        self._filter_column = None
        self._filter_value = None
        self._update_column = None

    def rowWhereColumn(self, column_name: str):
        """Setzt die Spalte für die WHERE-Bedingung."""
        self._filter_column = column_name
        return self  # <-- Die Magie! Gibt die Instanz selbst zurück.

    def equals(self, value: any):
        """Setzt den Wert für die WHERE-Bedingung."""
        self._filter_value = value
        return self  # <-- Erlaubt weitere Verkettung.

    def changeColumn(self, column_name: str):
        """Setzt die Spalte, die geändert werden soll."""
        self._update_column = column_name
        return self  # <-- Wieder `return self`.

    def to(self, new_value: any):
        """
        TERMINALE METHODE: Führt das Update aus und gibt den DataFrame zurück.
        Das ist das Ende der Kette.
        """
        # Validierung
        if not all([self._filter_column, self._update_column, self._filter_value is not None]):
            raise ValueError("Kette unvollständig. Bitte alle nötigen Methoden aufrufen.")
        
        # Ausführung
        df = spark.table(self._table_name)
        updated_df = df.withColumn(
            self._update_column,
            when(col(self._filter_column) == self._filter_value, lit(new_value))
            .otherwise(col(self._update_column))
        )
        
        return updated_df # <-- Gibt das Ergebnis zurück, nicht `self`

# Eine Factory-Funktion für einen sauberen Start
def doSet(table_name: str):
    return SparkTableUpdater(table_name)

# Erfolgreicher Aufruf
doSet("...").rowWhereColumn('Fileprefix').equals('SL_GP_Basis').changeColumn('primaryKeyColumns').to("Foo;bar")
```

**Bewertung:**
*   **Vorteil:** Einfach zu implementieren und zu verstehen.
*   **Nachteil:** Die Reihenfolge der Methoden wird nicht erzwungen. Der folgende Code ist syntaktisch korrekt, aber semantisch unsinnig und schwer lesbar. Außerdem schlägt die Validierung erst zur Laufzeit fehl.

```python
# Funktioniert, ist aber unleserlich
doSet("...").equals('SL_GP_Basis').changeColumn('primaryKeyColumns').rowWhereColumn('Fileprefix').to("Foo;bar")

# Führt zu einem Laufzeitfehler
doSet("...").equals('SL_GP_Basis').to("Foo;bar")
# -> ValueError: Kette unvollständig.
```

### Ansatz 2: Der State-Machine-Ansatz mit internen Klassen

Um die Reihenfolge zu erzwingen und die Entwicklererfahrung (DX) durch Auto-Complete in der IDE zu verbessern, können wir das Konzept einer State-Machine verwenden. Jede Methode gibt eine Instanz einer neuen, internen Klasse zurück, die nur die für den nächsten Schritt erlaubten Methoden anbietet.

```python
class SparkTableUpdater2:
    """
    Eine zustandsbasierte Fluent API, die die Reihenfolge der Methoden erzwingt.
    """
    # --- Schritt 1: Der Einstiegspunkt (äußere Klasse) ---
    def __init__(self, table_name: str):
        self._table_name = table_name
        self._filter_column = None
        self._filter_value = None
        self._update_column = None

    def rowWhereColumn(self, column_name: str):
        """Spezifiziert die Spalte für die WHERE-Klausel."""
        self._filter_column = column_name
        return self._ConditionBuilder(self) # Gibt eine Instanz für den nächsten Zustand zurück

    # --- Schritt 2: Interne Klasse für die Bedingung ---
    class _ConditionBuilder:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            self.outer = updater_instance

        def equals(self, value: any):
            """Spezifiziert den Wert für den Gleichheits-Check."""
            self.outer._filter_value = value
            return self.outer._TargetColumnSelector(self.outer) # Nächster Zustand

    # --- Schritt 3: Interne Klasse für die Zielspalte ---
    class _TargetColumnSelector:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            self.outer = updater_instance
        
        def changeColumn(self, column_name: str):
            """Spezifiziert die zu ändernde Spalte."""
            self.outer._update_column = column_name
            return self.outer._UpdateExecutor(self.outer) # Nächster Zustand

    # --- Schritt 4: Interne Klasse für die finale Ausführung ---
    class _UpdateExecutor:
        def __init__(self, updater_instance: "SparkTableUpdater2"):
            self.outer = updater_instance

        def to(self, new_value: any):
            """TERMINALE METHODE: Führt das Update aus."""
            df = self.outer.spark.table(self.outer._table_name)
            condition = (col(self.outer._filter_column) == self.outer._filter_value)
            
            return df.withColumn(
                self.outer._update_column,
                when(condition, lit(new_value)).otherwise(col(self.outer._update_column))
            )
```

**Profi-Tipp:** In den internen Klassen existiert der Typ `SparkTableUpdater2` zum Zeitpunkt der Definition der `__init__`-Signatur noch nicht. Deshalb verwenden wir einen *Forward Reference* mit Anführungszeichen: `updater_instance: "SparkTableUpdater2"`. Das signalisiert dem Type-Checker, dass der Typ später aufgelöst wird.

**Bewertung:**
*   **Vorteil:** Typsicher und robust. Die IDE bietet für jeden Schritt nur die korrekten Methoden an (`doSet(...).` + `TAB` zeigt nur `rowWhereColumn`). Falsche Reihenfolgen führen zu einem Fehler direkt im Editor, nicht erst zur Laufzeit.
*   **Nachteil:** Relativ viel "Boilerplate"-Code für die Definition der Zustände und Klassen.

### Deep Dive: Dynamische Ansätze in Python

Für Puristen, die Boilerplate-Code vermeiden wollen, bietet Python dynamische "Magic Methods", um das gleiche Ziel mit weniger Code zu erreichen. **Achtung:** Diese Ansätze sind eher akademischer Natur und haben praktische Nachteile.

#### Ansatz 3: Die `__getattr__`-Magie

Python ruft die `__getattr__(self, name)`-Methode auf einem Objekt auf, wenn ein Attribut (`name`) nicht auf normalem Wege gefunden wurde. Wir können diese Methode "kapern", um unsere API dynamisch zu steuern.

```python
from functools import partial

class DynamicUpdater:
    # Definiert die erlaubten Methoden für jeden Zustand
    _VALID_METHODS = {
        'initial': {'rowWhereColumn'},
        'condition_set': {'equals', 'isGreaterThan'},
        'operator_set': {'changeColumn'},
        'target_set': {'to'}
    }

    def __init__(self, table_name: str):
        self._state = 'initial'
        self._query_parts = {}
        # ... (andere Attribute wie _table_name)

    def __getattr__(self, name):
        # Wird nur aufgerufen, wenn `name` kein existierendes Attribut ist.
        if name in self._VALID_METHODS.get(self._state, set()):
            # Methode ist für den aktuellen Zustand gültig.
            # Wir geben eine Funktion zurück, die die Logik ausführt.
            return partial(self._dispatcher, name)
        
        # Methode ist ungültig -> Fehler
        raise AttributeError(f"Methode '{name}' ist im Zustand '{self._state}' nicht erlaubt.")

    def _dispatcher(self, method_name, *args, **kwargs):
        """Zentraler Handler für die Logik der Methoden."""
        if method_name == 'rowWhereColumn':
            self._query_parts['filter_col'] = args[0]
            self._state = 'condition_set' # Zustandsübergang
        # ... (Logik für andere Methoden)
        
        # Am Ende der Kette, gibt Ergebnis zurück
        elif method_name == 'to':
            return self._execute_update(args[0])

        return self # Kette fortsetzen

    def _execute_update(self, new_value):
        # ... (die eigentliche Spark-Logik)
        pass
```

**Bewertung:**
*   **Vorteil:** Sehr wenig Code (DRY - Don't Repeat Yourself). Die API ist datengesteuert.
*   **Nachteil:** **Ein massiver Nachteil:** Die statische Analyse und die IDE-Unterstützung gehen komplett verloren. Die IDE weiß nicht, welche Methoden existieren, da sie zur Laufzeit erzeugt werden. Fehler werden erst zur Laufzeit entdeckt, was die Entwicklererfahrung deutlich verschlechtert.

#### Ansatz 4: Metaprogrammierung mit Class Factories (`__new__`)

Der extremste Ansatz nutzt die *Magic Method* `__new__`, um Klassen nicht nur zu instanziieren, sondern zur Laufzeit komplett neu zu *erzeugen*. Wir definieren unsere API in einer Datenstruktur (z.B. einem Dictionary) und eine Factory-Funktion generiert daraus dynamisch die passenden Klassen für jeden Zustand.

```python
# Die API wird als Datenstruktur definiert
API_BLUEPRINT = {
    'initial': { 'methods': {'rowWhereColumn': ('filter_col', 'condition_builder')} },
    'condition_builder': { 'methods': {'equals': ('filter_val', 'target_selector')} },
    # ... und so weiter
}

class FluentAPIFactory:
    def __new__(cls, state_name: str):
        """Erzeugt dynamisch einen Klassentyp für einen gegebenen Zustand."""
        
        state_blueprint = API_BLUEPRINT[state_name]
        class_attributes = {}
        
        # __init__ für jede dynamische Klasse hinzufügen
        def __init__(self, query_parts, ...):
            self.query_parts = query_parts
            # ...
        class_attributes['__init__'] = __init__

        # Methoden basierend auf dem Blueprint dynamisch erzeugen
        for method_name, (key, next_state) in state_blueprint['methods'].items():
            def make_handler(...):
                # Closure, die den nächsten Zustand instanziiert
                def handler(self, value):
                    # ...
                    NextStateClass = cls(next_state)
                    return NextStateClass(...)
                return handler
            class_attributes[method_name] = make_handler(...)
            
        # Die neue Klasse mit type() erzeugen und zurückgeben
        return type(f"State_{state_name}", (object,), class_attributes)

# Der User-Facing Entry Point
def doSetWithFactory(table_name: str):
    InitialStateClass = FluentAPIFactory('initial')
    return InitialStateClass(query_parts={}, ...)
```

**Bewertung:**
*   **Vorteil:** Maximale Trennung von API-Definition und Implementierung. Extrem DRY.
*   **Nachteil:** Enorm hohe Komplexität. Schwer zu verstehen, zu debuggen und zu warten. Dieser Ansatz ist nur für Framework-Entwickler sinnvoll, nicht für ein alltägliches Werkzeug.

### Fazit und Empfehlung

Wir haben vier Wege zur Erstellung einer Fluent API gesehen, jeder mit seinen eigenen Vor- und Nachteilen.

| Ansatz | Komplexität | IDE-Support | Robustheit | Empfehlung |
| :--- | :--- | :--- | :--- | :--- |
| **1. Simple `return self`** | Sehr niedrig | Mäßig | Niedrig | Für sehr simple, private Helfer. |
| **2. State-Machine (interne Klassen)** | Mittel | **Hervorragend** | **Sehr hoch**| **Die empfohlene Lösung!** |
| **3. Dynamisch (`__getattr__`)** | Mittel | **Sehr schlecht** | Mittel | Akademisches Beispiel, für Tooling vermeiden. |
| **4. Class Factory (`__new__`)** | Sehr hoch | Schlecht | Hoch | Nur für Framework-Entwicklung. |

Für interne Werkzeuge, die von einem Team genutzt werden, ist der **State-Machine-Ansatz mit internen Klassen (Ansatz 2)** die bei weitem beste Wahl. Er bietet die perfekte Balance aus Lesbarkeit, Sicherheit und einer hervorragenden Entwicklererfahrung (DX) durch volle IDE-Unterstützung. Der etwas höhere initiale Schreibaufwand zahlt sich durch weniger Fehler und leichtere Wartbarkeit schnell aus.
