package test.oracle;


import java.util.List;
import org.mif.manager.capaoracle.GestorBDManagerJdbc;
import org.mif.manager.interficiepersistencia.GestorBDManagerException;
import org.mif.manager.model.*;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */

/**
 *
 * @author markos
 */
public class Test_funcions {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws GestorBDManagerException
    {
        GestorBDManagerJdbc dbm = new GestorBDManagerJdbc();
        List<Temporada> temps = dbm.obtenirTemporades();
        
        Equip noueq = new Equip();
        Categoria c = dbm.obtenirCategoria("Benjamí");
        System.out.println(c);
        noueq.setCategoria(c);
        noueq.setNom("Asaltayayas UD");
        noueq.setTipus("H");
        noueq.setTemporada(temps.get(0).getAnny());
        
        System.out.println(dbm.afegirEquip(noueq));
        
        List<Equip> tots_equips = dbm.obtenirEquips();
        for (int i = 0; i < tots_equips.size(); i++)
        {
            System.out.println(tots_equips.get(i));
        }
    }
}
