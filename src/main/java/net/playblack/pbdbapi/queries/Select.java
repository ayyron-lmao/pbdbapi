/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.playblack.pbdbapi.queries;

/**
 *
 * @author somners
 */
public class Select extends Where<Select> {

    @Override
    public Type getType() {
        return Type.SELECT;
    }
    
}
