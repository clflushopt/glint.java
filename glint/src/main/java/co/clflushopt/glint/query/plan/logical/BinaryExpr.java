package co.clflushopt.glint.query.plan.logical;

public abstract class BinaryExpr {
    private String name;
    private String operator;
    private LogicalExpr lhs;
    private LogicalExpr rhs;

    public BinaryExpr(String name, String operator, LogicalExpr lhs, LogicalExpr rhs) {
        this.name = name;
        this.operator = operator;
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", lhs.toString(), this.operator, rhs.toString());
    }

    public String getName() {
        return name;
    }

    public String getOperator() {
        return operator;
    }

    public LogicalExpr getLhs() {
        return lhs;
    }

    public LogicalExpr getRhs() {
        return rhs;
    }
}
