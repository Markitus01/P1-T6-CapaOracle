/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.mif.manager.capaoracle;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.mif.manager.interficiepersistencia.GestorBDManagerException;
import org.mif.manager.interficiepersistencia.IGestorBDManager;
import org.mif.manager.model.Categoria;
import org.mif.manager.model.Equip;
import org.mif.manager.model.Jugador;
import org.mif.manager.model.Temporada;
import org.mif.manager.model.Usuari;

/**
 *
 * @author markos
 */
public class GestorBDManagerJdbc implements IGestorBDManager
{
    /*
     * Aquest objecte és el que ha de mantenir la connexió amb el SGBD Es crea
     * en el constructor d'aquesta classe i manté la connexió fins que el
     * programador decideix tancar la connexió amb el mètode tancarCapa
     */
    private Connection conn;

    /**
     * Sentències preparades que es crearan només 1 vegada i s'utilitzaran
     * tantes vegades com siguin necessàries. En aquest programa es creen la
     * primera vegada que es necessiten i encara no han estat creades. També es
     * podrien crear al principi del programa, una vegada establerta la
     * connexió.
     */
    private PreparedStatement psSelListEquip;
    private PreparedStatement psSelEq;
//    private PreparedStatement psDelListProduct;
//    private PreparedStatement psUpdateProduct;
//    private PreparedStatement psInsertProduct;
    
    /**
     * Intenta connectar amb la BD utilitzant les credencials de l'arxiu XML
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    public GestorBDManagerJdbc() throws GestorBDManagerException
    {
        String nomFitxer = "managerJDBC.xml";
        
        try 
        {
            Properties props = new Properties();
            props.loadFromXML(new FileInputStream(nomFitxer));
            String[] claus = {"url", "user", "password"};
            String[] valors = new String[3];
            for (int i = 0; i < claus.length; i++) {
                valors[i] = props.getProperty(claus[i]);
                if (valors[i] == null || valors[i].isEmpty()) {
                    throw new GestorBDManagerException("L'arxiu " + nomFitxer + " no troba la clau " + claus[i]);
                }
            }
            conn = DriverManager.getConnection(valors[0], valors[1], valors[2]);
            conn.setAutoCommit(false);
        }
        catch (IOException ex)
        {
            throw new GestorBDManagerException("No s'ha pogut recuperar correctament l'arxiu de configuració " +nomFitxer, ex);
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("No es pot establir la connexió.", ex);
        }
    }
    
    
    
    /**
     * Retorna el llistat de temporades de la BD
     * 
     * @return Llista de temporades recuperades
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Temporada> obtenirTemporades() throws GestorBDManagerException
    {
        List<Temporada> temps = new ArrayList<Temporada>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT anny FROM temporada");
            while (rs.next()) {
                LocalDate anny = rs.getDate("anny").toLocalDate();
                Temporada t = new Temporada(anny);
                temps.add(t);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de temporades.\n", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de temporades.\n", ex);
                }
            }
        }
        return temps;
    }
    
    /**
     * Retorna el llistat de tots els equips de la BD
     * 
     * @return Llista d'equips recuperats
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    @Override
    public List<Equip> obtenirEquips() throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<Equip>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT id, nom, tipus, nom_cat, temporada FROM equip");
            while (rs.next()) {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                Equip e = new Equip();
                e.setId(id);
                e.setNom(nom);
                e.setTipus(tipus);
                e.setCategoria(nom_cat);
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de temporades.\n", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de temporades.\n", ex);
                }
            }
        }
        return equips;
    }
    
    /**
     * Retorna un llistat amb els equips que tinguin la String introduida al nom i/o siguin del tipus introduït
     * 
     * @param nomEquip Nom de l'equip que es vol cercar
     * @param tipus Tipus de l'equip que es vol cercar
     * @return Llistat d'equips que concordin amb nom i/o tipus
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Equip> buscarEquips(String nomEquip, String tipusEquip) throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<Equip>();
        
        if (psSelListEquip == null) {
            try {
                psSelListEquip = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE nom LIKE ? AND tipus LIKE ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psDelListProduct", ex);
            }
        }
        
        try
        {
            psSelListEquip.setString(1, "%"+nomEquip+"%");
            psSelListEquip.setString(2, "%"+tipusEquip+"%");
            ResultSet rs = psSelListEquip.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                Equip e = new Equip();
                e.setId(id);
                e.setNom(nom);
                e.setTipus(tipus);
                e.setCategoria(nom_cat);
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en eliminar els productes de codi indicat", ex);
        }
        
        return equips;
    }
    
    /**
     * Retorna l'equip obtingut a partir de la seva id
     * 
     * @param idEquip id de l'equip a obtenir
     * @return Equip obtingut
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Equip obtenirEquip(int idEquip) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Insereix l'equip a la BD
     * 
     * @param e Equip a inserir
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void afegirEquip(Equip e) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Modifica l'equip a la BD
     * 
     * @param e Equip a modificar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void modificarEquip(Equip e) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Esborra l'equip de la BD
     * 
     * @param e Equip a eliminar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void eliminarEquip(Equip e) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna tots els jugadors de la BD
     * 
     * @return Llista de tots els jugadors
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Jugador> obtenirJugadors() throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna llistat amb els jugadors que concordin amb nom/cognoms, equip o sexe introduits per cercar
     * 
     * @param nomJugador Nom o Cognoms dels jugadors o jugador que es volen cercar
     * @param e Equip del qual es volen obtenir els o el jugador
     * @param sexe Sexe del jugador o jugadors que es volen cercar
     * @return Llistat amb els jugadors (o jugador) que compleixin les condicions introduides
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Jugador> buscarJugadors(String nomJugador, Equip e, String sexe) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna el jugador obtingut a partir de la seva id
     * @param idJugador id del jugador a obtenir
     * @return Jugador obtingut
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Jugador obtenirJugador(int idJugador) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Insereix jugador a la BD
     * 
     * @param j Jugador a inserir
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void afegirJugador(Jugador j) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Modifica el jugador a la BD
     * 
     * @param j Jugador a modificar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void modificarJugador(Jugador j) throws GestorBDManagerException
    {
        
    }
    
    /**
     * ESborra el jugador de la BD
     * 
     * @param j Jugador a eliminar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void eliminarJugador(Jugador j) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna llistat amb totes les categories existents
     * 
     * @return Llista de totes les categories a la BD
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Categoria> obtenirCategories() throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna la categoria trobada a partir del seu id
     * 
     * @param categoria id de la categoria
     * @return Categoria obtenida
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Categoria obtenirCategoria(int categoria) throws GestorBDManagerException
    {
        
    }
    
    /**
     * Retorna l'usuari administrador de la BD loguejat amb login i contrasenya
     * 
     * @param login Identificador del administrador (mail)
     * @param pswd Contrasenya del administrador
     * @return Usuari administrador
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Usuari retornarUsuari(String login, String pswd) throws GestorBDManagerException
    {
        if (conn != null)
        {
            
        }
    }
    
    /**
     * Tanca la conexió amb la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void tancarCapa() throws GestorBDManagerException
    {
        if (conn != null)
        {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en fer el rollback final.\n", ex);
            }
            try {
                conn.close();
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en tancar la connexió.\n", ex);
            }
        }
    }
    
    /**
     * Conserva els canvis efectuats a la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void confirmarCanvis() throws GestorBDManagerException
    {
        try {
            conn.commit();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en confirmar canvis.\n", ex);
        }
    }
    
    /**
     * Desfa els canvis efectuats a la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    @Override
    public void desferCanvis() throws GestorBDManagerException
    {
        try {
            conn.rollback();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en desfer canvis.\n", ex);
        }
    }
}
