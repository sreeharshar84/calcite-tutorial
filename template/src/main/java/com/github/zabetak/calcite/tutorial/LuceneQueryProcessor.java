/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.github.zabetak.calcite.tutorial.rules.LuceneToEnumerableConverterRule;
import com.github.zabetak.calcite.tutorial.rules.LuceneTableScanRule;
import com.github.zabetak.calcite.tutorial.rules.LuceneFilterRule;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * Query processor for running TPC-H queries over Apache Lucene.
 */
public class LuceneQueryProcessor {

  /**
   * Plans and executes an SQL query in the file specified
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: processor SQL_FILE");
      System.exit(-1);
    }

    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

    // 1. Create the root schema and type factory
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // 2. Create the data type for each TPC-H table
    for (TpchTable tpchTable: TpchTable.values()) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (TpchTable.Column c : tpchTable.columns) {
        builder.add(c.name, typeFactory.createJavaType(c.type).getSqlTypeName());
      }
      // 3. Add the TPC-H table to the schema
      String indexPath = Paths.get(DatasetIndexer.INDEX_LOCATION, "tpch",
              tpchTable.name()).toString();
      System.out.println("indexPath: " + indexPath);
      schema.add(tpchTable.name(), new LuceneTable(indexPath, builder.build()));
    }

    // 4. Create an SQL parser
    SqlParser parser = SqlParser.create(sqlQuery);
    // 5. Parse the query into an AST
    // 6. Print and check the AST
    SqlNode parserAst = parser.parseQuery();
    System.out.println("Parsed Qfuery1");
    System.out.println(parserAst.toString());

    // 7. Configure and instantiate the catalog reader
    CalciteConnectionConfig readerConfig =
            CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
            Collections.emptyList(), typeFactory, readerConfig);

    // 8. Create the SQL validator using the standard operator table and default configuration
    SqlValidator sqlValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
            catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
    // 9. Validate the initial AST
    SqlNode validAst = sqlValidator.validate(parserAst);
    System.out.println("\n-------Validated Qfuery1---------");
    System.out.println(validAst.toString());

    // 10. Create the optimization cluster to maintain planning information
    RelOptCluster cluster = newCluster(typeFactory);

    // 11. Configure and instantiate the converter of the AST to Logical plan
    // - No view expansion (use NOOP_EXPANDER)
    // - Standard expression normalization (use StandardConvertletTable.INSTANCE)
    // - Default configuration (SqlToRelConverter.config())
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(NOOP_EXPANDER,
            sqlValidator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());

    // 12. Convert the valid AST into a logical plan
    RelNode logPlan = sqlToRelConverter.convertQuery(validAst, false, true).rel;

    // 13. Display the logical plan with explain attributes
    System.out.println(RelOptUtil.dumpPlan("\n-------Logical Pflan---------", logPlan,
            SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES));

    // 14. Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();

    // 15. Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    //planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

    planner.addRule(LuceneTableScanRule.DEFAULT.toRule());
    planner.addRule(LuceneToEnumerableConverterRule.DEFAULT.toRule());
    //Pushdown
    planner.addRule(LuceneFilterRule.DEFAULT.toRule());

    // 16. Start the optimization process to obtain the most efficient physical plan based on
    // the provided rule set.
    logPlan = planner.changeTraits(logPlan, logPlan.getTraitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);
    EnumerableRel physicalPlan = (EnumerableRel) planner.findBestExp();

    // 17. Display the physical plan
    System.out.println(RelOptUtil.dumpPlan("\n-------Physical Pflan---------", physicalPlan,
            SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES));

    // 18. Compile generated code and obtain the executable program
    Bindable<Object []> execPlan = EnumerableInterpretable.toBindable(new HashMap<>(),
            null,
            physicalPlan,
            EnumerableRel.Prefer.ARRAY);

    // 19. Run the program using a context simply providing access to the schema and print
    // results
    long start = System.currentTimeMillis();

    for (Object row: execPlan.bind(new SchemaOnlyDataContext(schema))) {
      if (row instanceof Object[]) {
        System.out.println(Arrays.toString((Object[]) row));
      } else {
        System.out.println(row);
      }
    }

    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
