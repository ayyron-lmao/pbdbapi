package net.playblack.pbdbapi.queries;

/**
 *
 * @author somners
 */
public class Delete extends Where<Delete> {

    @Override
    public Type getType() {
        return Type.DELETE;
    }

}
