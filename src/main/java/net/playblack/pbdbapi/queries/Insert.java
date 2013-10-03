package net.playblack.pbdbapi.queries;

/**
 *
 * @author somners
 */
public class Insert extends Where<Insert> {

    @Override
    public Type getType() {
        return Type.INSERT;
    }
}
