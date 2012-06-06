/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import optimizers.*;
import components.Component;
import components.DataSourceComponent;
import components.EquiJoinComponent;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.SelectOperator;
import queryPlans.QueryPlan;
import schema.Schema;
import util.HierarchyExtractor;
import util.ParserUtil;
import util.TableAliasName;
import visitors.squall.JoinHashVisitor;
import visitors.squall.WhereVisitor;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class NameComponentGenerator implements ComponentGenerator{
    private TableAliasName _tan;
    private OptimizerTranslator _ot;

    private Schema _schema;
    private String _dataPath;
    private String _extension;

    private QueryPlan _queryPlan = new QueryPlan();
    //List of Components which are already added throughEquiJoinComponent and OperatorComponent
    private List<Component> _subPlans = new ArrayList<Component>();

    //compName, CostParameters for all the components from _queryPlan
    private Map<String, CostParameters> _compCost =  new HashMap<String, CostParameters>();

    //used for WHERE clause
    private final HashMap<String, Expression> _compNamesAndExprs;
    private final HashMap<Set<String>, Expression> _compNamesOrExprs;

    public NameComponentGenerator(Schema schema,
            TableAliasName tan,
            OptimizerTranslator ot,
            String dataPath,
            String extension,
            HashMap<String, Expression> compNamesAndExprs,
            HashMap<Set<String>, Expression> compNamesOrExprs){
        _schema = schema;
        _tan = tan;
        _ot = ot;
        _dataPath = dataPath;
        _extension = extension;

        _compNamesAndExprs = compNamesAndExprs;
        _compNamesOrExprs = compNamesOrExprs;
    }

    @Override
    public QueryPlan getQueryPlan(){
        return _queryPlan;
    }

    @Override
    public List<Component> getSubPlans(){
        return _subPlans;
    }

    /*
     * adding a DataSourceComponent to the list of components
     * Necessary to call only when only one table is addresses in WHERE clause of a SQL query
     */
    @Override
    public DataSourceComponent generateDataSource(String tableCompName){
        DataSourceComponent source = createAddDataSource(tableCompName);
        createCompCost(source);
        return source;
    }

    private DataSourceComponent createAddDataSource(String tableCompName) {
        String tableSchemaName = _tan.getSchemaName(tableCompName);
        String sourceFile = tableSchemaName.toLowerCase();

        DataSourceComponent relation = new DataSourceComponent(
                                        tableCompName,
                                        _dataPath + sourceFile + _extension,
                                        _queryPlan);
        _subPlans.add(relation);
        return relation;
    }

    /*
     * Setting cardinality and schema for DataSourceComponent
     */
    private void createCompCost(DataSourceComponent source) {
        String compName = source.getName();
        CostParameters costParams = new CostParameters();

        costParams.setCardinality(_schema.getTableSize(compName));
        costParams.setSchema(_schema.getTableSchema(compName));

        _compCost.put(compName, costParams);
    }

    /*
     * Join between two components
     * List<Expression> is a set of join conditions between two components.
     */
    @Override
    public Component generateEquiJoin(Component left, Component right, List<Expression> joinCondition){
        EquiJoinComponent joinComponent = new EquiJoinComponent(
                    left,
                    right,
                    _queryPlan);

        //set hashes for two parents
        addHash(left, joinCondition);
        addHash(right, joinCondition);

        _subPlans.remove(left);
        _subPlans.remove(right);
        _subPlans.add(joinComponent);

        return joinComponent;
    }

    /*************************************************************************************
     * WHERE clause - SelectOperator
     *************************************************************************************/
    public void addSelectOperator(Component component){
        Expression whereCompExpr = createWhereForComponent(component);
        processWhereForComponent(whereCompExpr, component);
    }

    /*
     * Merging atomicExpr and orExpressions corresponding to this component
     */
    private Expression createWhereForComponent(Component component){
        Expression expr = _compNamesAndExprs.get(component.getName());

        for(Map.Entry<Set<String>, Expression> orEntry: _compNamesOrExprs.entrySet()){
            Set<String> orCompNames = orEntry.getKey();

            if(HierarchyExtractor.isLCM(component, orCompNames)){
                Expression orExpr = orEntry.getValue();
                if (expr != null){
                    //appending to previous expressions
                    expr = new AndExpression(expr, orExpr);
                }else{
                    //this is the first expression for this component
                    expr = orExpr;
                }
            }
        }
        return expr;
    }

    /*
     * whereCompExpression is the part of WHERE clause which refers to affectedComponent
     * This is the only method in this class where WhereVisitor is actually instantiated and invoked
     */
    private void processWhereForComponent(Expression whereCompExpression, Component affectedComponent){
        WhereVisitor whereVisitor = new WhereVisitor(_queryPlan, affectedComponent, _schema, _tan, _ot);
        whereCompExpression.accept(whereVisitor);
        attachWhereClause(whereVisitor.getSelectOperator(), affectedComponent);
    }

    private void attachWhereClause(SelectOperator select, Component affectedComponent) {
        affectedComponent.addOperator(select);
    }

    //set hash for this component, knowing its position in the query plan.
    //  Conditions are related only to parents of join,
    //  but we have to filter who belongs to my branch in JoinHashVisitor.
    //  We don't want to hash on something which will be used to join with same later component in the hierarchy.
    private void addHash(Component component, List<Expression> joinCondition) {
            JoinHashVisitor joinOn = new JoinHashVisitor(_schema, _queryPlan, component, _tan, _ot);
            for(Expression exp: joinCondition){
                exp.accept(joinOn);
            }
            List<ValueExpression> hashExpressions = joinOn.getExpressions();

            if(!joinOn.isComplexCondition()){
                //all the join conditions are represented through columns, no ValueExpression (neither in joined component)
                //guaranteed that both joined components will have joined columns visited in the same order
                //i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B), respectively
                List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

                //hash indexes in join condition
                component.setHashIndexes(hashIndexes);
            }else{
                //hahs expressions in join condition
                component.setHashExpressions(hashExpressions);
            }
    }

}