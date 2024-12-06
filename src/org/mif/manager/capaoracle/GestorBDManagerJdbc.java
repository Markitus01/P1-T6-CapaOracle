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
import java.sql.Statement;
import java.sql.Date;
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
    private PreparedStatement psInsEq;
    private PreparedStatement psModEq;
    private PreparedStatement psSelCat;
    
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
                e.setCategoria(obtenirCategoria(nom_cat));
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en intentar recuperar la llista d'equips\n", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista d'equips.\n", ex);
                }
            }
        }
        return equips;
    }
    
    /**
     * Retorna un llistat amb els equips que tinguin la String introduida al nom i/o siguin del tipus introduït
     * 
     * @param nomEquip Nom de l'equip que es vol cercar
     * @param tipusEquip Tipus de l'equip que es vol cercar
     * @return Llistat d'equips que concordin amb nom i/o tipus
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Equip> buscarEquips(String nomEquip, String tipusEquip) throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<>();
        
        if (psSelListEquip == null) {
            try {
                psSelListEquip = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE nom LIKE ? AND tipus LIKE ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psSelListEquip", ex);
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
                e.setCategoria(obtenirCategoria(nom_cat));
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en seleccionar els equips de nom i/o tipus indicat", ex);
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
        Equip equip = new Equip();
        
        if (psSelEq == null) {
            try {
                psSelEq = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psSelEq", ex);
            }
        }
        
        try
        {
            psSelEq.setInt(1, idEquip);
            ResultSet rs = psSelEq.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                equip.setId(id);
                equip.setNom(nom);
                equip.setTipus(tipus);
                equip.setCategoria(obtenirCategoria(nom_cat));
                equip.setTemporada(anny);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en seleccionar l'equip amb id indicat", ex);
        }
        
        return equip;
    }
    
    /**
     * Insereix l'equip a la BD
     * 
     * @param e Equip a inserir
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int afegirEquip(Equip e) throws GestorBDManagerException
    {
        int inserit = 0;
        
        if (psInsEq == null) {
            try {
                psInsEq = conn.prepareStatement("INSERT INTO equip (nom, tipus, nom_cat, temporada) VALUES (?,?,?,?)");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psInsEq", ex);
            }
        }
        
        try
        {
            psInsEq.setString(1, e.getNom());
            psInsEq.setString(2, e.getTipus());
            psInsEq.setString(3, e.getCategoria().getNom());
            psInsEq.setDate(4, Date.valueOf(e.getTemporada()));
            inserit = psInsEq.executeUpdate();

        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en inserir l'equip", ex);
        }
        
        return inserit;
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
        return null;
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
        return null;
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
        return null;
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
        return null;
    }
    
    /**
     * Retorna la categoria trobada a partir del seu nom
     * 
     * @param nom_busqueda Nom de la categoria a buscar
     * @return Categoria obtenida
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Categoria obtenirCategoria(String nom_busqueda) throws GestorBDManagerException
    {
        Categoria cat = new Categoria();
        
        if (psSelCat == null) {
            try {
                psSelCat = conn.prepareStatement("SELECT id, nom, edat_min, edat_max FROM categoria WHERE nom = ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psSelCat", ex);
            }
        }
        
        try
        {
            psSelCat.setString(1, nom_busqueda);
            ResultSet rs = psSelCat.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                Integer edat_min = rs.getInt("edat_min");
                Integer edat_max = rs.getInt("edat_max");
                cat.setId(id);
                cat.setNom(nom);
                cat.setEdat_min(edat_min);
                // Si edat max es 0 vol dir que a la BD es null, per tant asignem 99 com a edat máxima
                if (edat_max == 0)
                {
                    cat.setEdat_max(99);
                }
                else
                {
                    cat.setEdat_max(edat_max);
                }
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en obtenir categoria amb el nom indicat", ex);
        }
        
        return cat;
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
        return null;
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